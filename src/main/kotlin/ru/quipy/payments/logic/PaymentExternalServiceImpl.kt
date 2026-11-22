package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        const val DEADLINE_MARGIN_MILLIS = 150L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val rateWaitLock = ReentrantLock(true)
    private val parallelRequestsSemaphore = Semaphore(parallelRequests, true)

    private val client = OkHttpClient.Builder().build()

    private val throughputPerMillis = minOf(
        parallelRequests.toDouble() / requestAverageProcessingTime.toMillis(),
        rateLimitPerSec / 1000.0,
    )

    private val inFlight = AtomicInteger(0)
    private val processing = AtomicInteger(0)

    private val outgoingCounter = Counter.builder("payments.outgoing")
        .description("Requests sent to the external payment system")
        .tag("account", accountName)
        .register(meterRegistry)

    init {
        Gauge.builder("payments.queue.size", inFlight) { it.get().toDouble() }
            .description("Accepted payments that are not completed yet")
            .tag("account", accountName)
            .register(meterRegistry)
        Gauge.builder("payments.account.capacity") { throughputPerMillis * 1000 }
            .description("Theoretical account throughput, requests per second")
            .tag("account", accountName)
            .register(meterRegistry)
    }

    private fun completed(result: String) {
        Counter.builder("payments.completed")
            .tag("account", accountName)
            .tag("result", result)
            .register(meterRegistry)
            .increment()
    }

    override fun tryReserve(deadline: Long): Long {
        while (true) {
            val current = inFlight.get()
            val waiting = (current - processing.get()).coerceAtLeast(0)
            val processingMillis = requestAverageProcessingTime.toMillis()
            val expectedDuration = if (deadline - now() <= processingMillis * 4) {
                rateLimiter.estimateWaitMillis(waiting) + processingMillis * 2.0 + DEADLINE_MARGIN_MILLIS
            } else {
                waiting / throughputPerMillis + processingMillis + DEADLINE_MARGIN_MILLIS
            }
            val expectedFinish = now() + expectedDuration.toLong()
            if (expectedFinish > deadline) {
                return 1000L
            }
            if (inFlight.compareAndSet(current, current + 1)) return 0
        }
    }

    override fun release() {
        inFlight.decrementAndGet()
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        try {
            doPerformPayment(paymentId, amount, paymentStartedAt, deadline)
        } finally {
            release()
        }
    }

    private fun doPerformPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val remainingTime = deadline - now()

        if (remainingTime <= 0 || !parallelRequestsSemaphore.tryAcquire(remainingTime, TimeUnit.MILLISECONDS)) {
            logger.error("[$accountName] [ERROR] Parallel requests limit wait exceeded deadline, transactionID: $transactionId, paymentID: $paymentId")
            completed("deadline_exceeded")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded while waiting for parallel requests limit")
            }
            return
        }

        try {
            if (!awaitRatePermit(deadline)) {
                completed("deadline_exceeded")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded while waiting for rate limiter")
                }
                return
            }

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            outgoingCounter.increment()
            processing.incrementAndGet()
            try {
                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                    completed(if (body.result) "success" else "failure")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            } finally {
                processing.decrementAndGet()
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    completed("timeout")
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    completed("error")

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            parallelRequestsSemaphore.release()
        }
    }

    private fun awaitRatePermit(deadline: Long): Boolean {
        val remaining = deadline - now()
        if (remaining <= 0 || !rateWaitLock.tryLock(remaining, TimeUnit.MILLISECONDS)) return false
        try {
            while (now() < deadline) {
                if (rateLimiter.tick()) return true
                Thread.sleep(1)
            }
            return false
        } finally {
            rateWaitLock.unlock()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
