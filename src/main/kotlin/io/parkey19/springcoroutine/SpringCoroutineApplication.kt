package io.parkey19.springcoroutine

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonTypeInfo
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cache.CacheManager
import org.springframework.cache.annotation.Cacheable
import org.springframework.cache.annotation.CachingConfigurerSupport
import org.springframework.cache.annotation.EnableCaching
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.annotation.Id
import org.springframework.data.r2dbc.core.*
import org.springframework.data.redis.cache.CacheKeyPrefix
import org.springframework.data.redis.cache.RedisCacheConfiguration
import org.springframework.data.redis.cache.RedisCacheManager
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer
import org.springframework.kotlin.coroutine.EnableCoroutine
import org.springframework.kotlin.coroutine.annotation.Coroutine
import org.springframework.kotlin.coroutine.context.DEFAULT_DISPATCHER
import org.springframework.stereotype.Repository
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.reactive.function.client.WebClient
import java.io.Serializable
import java.time.Duration

@SpringBootApplication
class SpringCoroutineApplication

fun main(args: Array<String>) {
    runApplication<SpringCoroutineApplication>(*args)
}

@Configuration
class UserConfiguration {
    @Bean
    fun databaseInitializer(userRepository: UserRepository) = ApplicationRunner {
        runBlocking { userRepository.init() }
    }
}

@RestController
class UserWithDetailsController(
        private val userRepository: UserRepository,
        private val cacheManager: RedisCacheManager?
) {

    @GetMapping("/")
    fun findAll(): Flow<User> =
            userRepository.findAll()

    @GetMapping("/{id}")
    @Cacheable("UserCache")
//    @Cacheable(value = ["UserDto"], unless="#User == null", cacheManager = "cacheManager")
    suspend open fun findOne(@PathVariable id: String): UserDto {
        val user: User = userRepository.findOne(id) ?:
        throw CustomException("This user does not exist")
        return user.render()
    }

    fun User.render() = UserDto(id, name, age)
}

class CustomException(s: String) : Throwable(s)

data class User(@Id val id: Long, val name: String, val age: Int)

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
data class UserDto @JsonCreator constructor(
        @JsonProperty("id")
        val id: Long,
        @JsonProperty("name")
        val name: String,
        @JsonProperty("age")
        val age: Int
) : Serializable

@Repository
class UserRepository(private val client: DatabaseClient) {

    suspend fun count(): Long =
            client.execute("SELECT COUNT(*) FROM user")
                    .asType<Long>().fetch().awaitOne()

    fun findAll(): Flow<User> =
            client.select().from("user").asType<User>().fetch().flow()

    suspend fun findOne(id: String): User? =
            client.execute("SELECT * FROM user WHERE id = :id")
                    .bind("id", id).asType<User>()
                    .fetch()
                    .awaitOneOrNull()

    suspend fun deleteAll() =
            client.execute("DELETE FROM user").await()

    suspend fun save(user: User) =
            client.insert().into<User>().table("user").using(user).await()

    suspend fun init() {
        client.execute("CREATE TABLE IF NOT EXISTS user (id long PRIMARY KEY, name varchar, age int);").await()
        deleteAll()
        save(User(1L, "Stéphane", 30))
        save(User(2L, "Sébastien", 20))
        save(User(3L, "Brian", 10))
    }
}

@Configuration
@EnableCaching
@EnableCoroutine
class RedisConfigration : CachingConfigurerSupport() {
    @Bean
    fun cacheManager(connectionFactory: RedisConnectionFactory): RedisCacheManager? {
        val builder = RedisCacheManager.RedisCacheManagerBuilder.fromConnectionFactory(connectionFactory)
        val configuration = RedisCacheConfiguration.defaultCacheConfig()
                .disableCachingNullValues()
                .serializeKeysWith(RedisSerializationContext.SerializationPair.fromSerializer(StringRedisSerializer()))
                .serializeValuesWith(RedisSerializationContext.SerializationPair.fromSerializer(GenericJackson2JsonRedisSerializer()))
                .computePrefixWith(CacheKeyPrefix.simple())
                .entryTtl(Duration.ofMinutes(5L))
        builder.cacheDefaults(configuration)
        return builder.build()
    }
}