package com.hand.util.redis.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hand.util.redis.Field.PubClient;
import com.hand.util.json.JsonMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Created by Hand on 2016/11/11.
 */
@Configuration
public class RedisConfig {


    @Autowired
    private JedisConnectionFactory jedisConnectionFactory;

    @Bean(name="stringRedisTemplate")
    public StringRedisTemplate makeStringRedisTemplate(){
        StringRedisTemplate redisTemplate =  new StringRedisTemplate();
        redisTemplate.setConnectionFactory(jedisConnectionFactory);
        return redisTemplate;
    }

    @Bean(name="objectMapper")
    public ObjectMapper makeObjectMapper(){
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        return  mapper;
    }
    
    @Bean(name="jsonMapper")
    public JsonMapper  makeJsonMapper(){
    	return new JsonMapper();
    }

    @Bean(name="pubClient")
    public PubClient makePubCliet(){
        return new PubClient(jedisConnectionFactory.getHostName(),jedisConnectionFactory.getPort());
    }
}
