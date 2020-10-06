package io.parkey19.springcoroutine

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.runBlocking
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cache.annotation.CachingConfigurerSupport
import org.springframework.cache.annotation.EnableCaching
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.core.asType
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisOperations
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer
import org.springframework.http.MediaType
import org.springframework.kotlin.coroutine.EnableCoroutine
import org.springframework.stereotype.Component
import org.springframework.stereotype.Repository
import org.springframework.web.reactive.function.server.*
import org.springframework.web.reactive.function.server.ServerResponse.ok
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty
import java.time.Duration
import java.util.function.Consumer

@SpringBootApplication
class SpringCoroutineApplication

fun main(args: Array<String>) {
    runApplication<SpringCoroutineApplication>(*args)
}

@Configuration
class UserConfiguration {
    @Bean
    fun databaseInitializer(userRepository: UserRepository) = ApplicationRunner {
        runBlocking {
            userRepository.init().subscribe()
        }
    }
}

@Configuration
open class ProductRouter(private val handler: UserHandler) {
    @Bean
    fun route() = coRouter {
        "/".nest {
            accept(MediaType.APPLICATION_JSON).nest {
                GET("/{id}", handler::findOne)
            }
        }
    }
}

@Component
class UserHandler(
        private val userRepository: UserRepository,
        val contentRedisOps: ReactiveRedisOperations<String, User>
//        private val userService: UserService
) {
    companion object {
        const val MINUTE_TO_LIVE = 1L
    }

    suspend fun findOne(request: ServerRequest): ServerResponse {
        val id = request.pathVariable("id")
        val key = "user:${id}"

        val mono = contentRedisOps.opsForValue().get(key).switchIfEmpty {
            userRepository.findOne(id).doOnSuccess {
                contentRedisOps.opsForValue()
                        .set(key, it, Duration.ofDays(MINUTE_TO_LIVE))
                        .subscribe(Consumer { println("sdkfsadkdfjalskdjfksajflksajflkajslkfjaslkjflkasjflkjasf") })
            }
        }

        return ok()
            .json()
//            .body(mononono)
            .bodyAndAwait(mono.asFlow())
    }


    fun User.render() = UserDto(id, name, age)
}

//@Service
//class UserService(@Autowired val userRepository: UserRepository) {
//
//    @Cacheable("Users")
//    suspend open fun findO(id: String): User? {
//        return userRepository.findOne(id) ?:throw CustomException("This user does not exist")
//    }
//}

//@RestController
//class UserWithDetailsController(
//        private val userRepository: UserRepository,
//        private val userdetailsService: UserWithDetailsService
//) {
//
//    @GetMapping("/")
//    fun findAll(): Flow<User> =
//            userRepository.findAll()
//
//    @GetMapping("/{id}")
////    @Cacheable("UserCache")
////    @Cacheable(value = ["UserDto"], unless="#User == null", cacheManager = "cacheManager")
//    suspend fun findOne(@PathVariable id: String): UserDto {
////        val user: User = userRepository.findOne(id) ?:
////        throw CustomException("This user does not exist")
////        return user.render()
//        return userdetailsService.findOne(id)
//    }
//
//    fun User.render() = UserDto(id, name, age)
//}

class CustomException(s: String) : Throwable(s)

data class User (
        @JsonProperty("id")
        val id: Long,
        @JsonProperty("name")
        val name: String,
        @JsonProperty("age")
        val age: Int
)

data class UserDto (
        @JsonProperty("id")
        val id: Long,
        @JsonProperty("name")
        val name: String,
        @JsonProperty("age")
        val age: Int
)

@Repository
class UserRepository(private val client: DatabaseClient) {

    fun findAll() : Flux<User> {
		return client.select().from("user").asType<User>()
                .fetch().all();
	}

	fun findOne(id: String): Mono<User> {
		return client.execute("SELECT * FROM user WHERE id = :id")
			.bind("id", id).asType<User>()
                .fetch().one()
	}

    fun save(user: User?): Mono<Void> {
        return client.insert().into(User::class.java).table("user")
                .using(user!!).then()
    }

    fun deleteAll(): Mono<Void> {
        return client.execute("DELETE FROM user").then()
    }

    fun init() : Mono<Void> {
        return client.execute("CREATE TABLE IF NOT EXISTS user (id long PRIMARY KEY, name varchar, age int);")
                .then()
                .then(deleteAll())
                .then(save(User(1L, "Stéphane", 20)))
                .then(save(User(2L, "Sébastien", 22)))
                .then(save(User(3L, "Brian", 23)))
    }
}

@Configuration
@EnableCaching
@EnableCoroutine
class RedisConfigration : CachingConfigurerSupport() {
//    @Bean
//    fun cacheManager(connectionFactory: RedisConnectionFactory): RedisCacheManager? {
//        val objectMapper = ObjectMapper().registerModule(KotlinModule())
//        val builder = RedisCacheManager.RedisCacheManagerBuilder.fromConnectionFactory(connectionFactory)
//        val configuration = RedisCacheConfiguration.defaultCacheConfig()
//                .disableCachingNullValues()
//                .serializeKeysWith(RedisSerializationContext.SerializationPair.fromSerializer(StringRedisSerializer()))
//                .serializeValuesWith(RedisSerializationContext.SerializationPair.fromSerializer(GenericJackson2JsonRedisSerializer(objectMapper)))
//                .computePrefixWith(CacheKeyPrefix.simple())
//                .entryTtl(Duration.ofMinutes(5L))
//        builder.cacheDefaults(configuration)
//        return builder.build()
//    }
    @Primary
    @Bean("primaryRedisConnectionFactory")
    fun connectionFactory(): ReactiveRedisConnectionFactory? {
        val connectionFactory = LettuceConnectionFactory()
//        connectionFactory.database = database
        return connectionFactory
    }

    @Bean
    fun contentReactiveRedisTemplate(factory: ReactiveRedisConnectionFactory?): ReactiveRedisTemplate<String, User>? {
        val keySerializer = StringRedisSerializer()
        val redisSerializer = Jackson2JsonRedisSerializer(User::class.java)
                .apply {
                    setObjectMapper(
                            jacksonObjectMapper()
                                    .registerModule(JavaTimeModule())
                                    .registerModule(KotlinModule())
                    )
                }
        val serializationContext = RedisSerializationContext
                .newSerializationContext<String, User>()
                .key(keySerializer)
                .hashKey(keySerializer)
                .value(redisSerializer)
                .hashValue(redisSerializer)
                .build()
        return ReactiveRedisTemplate(factory!!, serializationContext)
    }
}