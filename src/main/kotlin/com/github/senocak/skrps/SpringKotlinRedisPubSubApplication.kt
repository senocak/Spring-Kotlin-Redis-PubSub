package com.github.senocak.skrps

import com.fasterxml.jackson.core.JsonProcessingException
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.pubsub.RedisPubSubAdapter
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.resource.ClientResources
import io.lettuce.core.resource.DefaultClientResources
import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.util.concurrent.CompletableFuture

@Configuration
class RedisConfig {
    @Value("\${app.redis.HOST}")
    private lateinit var host: String

    @Value("\${app.redis.PORT}")
    private var port: Int = 0

    @Value("\${app.redis.PASSWORD}")
    private lateinit var password: String

    @Value("\${app.redis.CHANNEL}")
    lateinit var channel: String

    @Bean
    fun clientResource(): ClientResources = DefaultClientResources.create()

    @Bean
    fun redisClient(pubSubRedisClientResource: ClientResources): RedisClient {
        val create = RedisURI.builder()
            .withHost(host)
            .withPort(port)
            .withPassword(password.toCharArray())
            .build()
        return RedisClient.create(pubSubRedisClientResource, create)
    }

    @Bean
    fun statefulRedisPubSubConnection(client: RedisClient): StatefulRedisPubSubConnection<String, String> =
        client.connectPubSub()
}

@SpringBootApplication
@RestController("pubsub")
class SpringKotlinRedisPubSubApplication(
    private val config: RedisConfig,
    private val pubRedisConnection: StatefulRedisPubSubConnection<String, String>,
    private val myRedisPubSubListener: MyRedisPubSubListener
){

    @PostConstruct
    fun sub() {
        pubRedisConnection.addListener(myRedisPubSubListener)
        pubRedisConnection.async().subscribe(config.channel)
    }

    @RequestMapping("/pub")
    fun pub(@RequestParam message: String) {
        publish(channel = config.channel, message = message)
    }

    fun publish(channel: String, message: String): CompletableFuture<Long?> {
        var future = CompletableFuture<Long?>()
        try {
            future = pubRedisConnection.async()
                .publish(channel, message)
                .toCompletableFuture()
        } catch (e: JsonProcessingException) {
            future.completeExceptionally(e)
        }
        return future
    }
}

@Component
class MyRedisPubSubListener: RedisPubSubAdapter<String, String>() {
    private val log: Logger by logger()
    override fun message(channel: String?, message: String?) {
        log.info("channel: $channel message: $message")
    }
    override fun subscribed(channel: String?, count: Long) {
        log.info("subscribed channel: $channel message: $count")
    }
    override fun psubscribed(pattern: String?, count: Long) {
        log.info("psubscribed pattern: $pattern message: $count")
    }
    override fun unsubscribed(channel: String?, count: Long) {
        log.info("unsubscribed channel: $channel message: $count")
    }
    override fun punsubscribed(pattern: String?, count: Long) {
        log.info("punsubscribed pattern: $pattern message: $count")
    }
}
fun main(args: Array<String>) {
    runApplication<SpringKotlinRedisPubSubApplication>(*args)
}
fun <R : Any> R.logger(): Lazy<Logger> = lazy {
    LoggerFactory.getLogger((if (javaClass.kotlin.isCompanion) javaClass.enclosingClass else javaClass).name)
}