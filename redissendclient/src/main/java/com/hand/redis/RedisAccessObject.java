/*
 * Copyright Hand China Co.,Ltd.
 */

package com.hand.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;


import javax.annotation.Resource;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * @author shengyang.zhou@hand-china.com
 */
public abstract class RedisAccessObject<T> {

    private static final String _LAST = "\uFFFF";

    private RedisTemplate<String,T> objRedisTemplate;

    protected Class<T> clazz;

    @Resource(name="objectMapper")
    protected ObjectMapper objectMapper;

    protected String hashTag = "";

    protected FieldDescriptor idDescriptor;

    @Resource(name="stringRedisTemplate")
    protected StringRedisTemplate redisTemplate;
    // Logger logger = LoggerFactory.getLogger(getClass());

    Map<String, FieldDescriptor> fieldDescriptorMap = new HashMap<>();

    public void addFieldDescriptor(FieldDescriptor descriptor) {
        fieldDescriptorMap.put(descriptor.getName(), descriptor);
    }

    /**
     * return total count of records
     *
     * @return
     */
    public int count() {
        String key = createPattern();
        //return jedis.hlen(key).intValue();
        return redisTemplate.boundHashOps(key).size().intValue();
    }

    public void addObject(T obj) {
        String id = getIdValue(obj);
        String pattern = createPattern();
        String json = convertToJson(obj);
//        Map<String, String> map = new HashMap<>();
//        map.put(id, json);
//        jedis.hmset(pattern, map);
        redisTemplate.opsForHash().put(pattern, id, json);

        fieldDescriptorMap.forEach((name, field) -> {
            String key;
            String value = String.valueOf(getFieldValue(field.field, obj));
            switch (field.getType()) {
            case FieldDescriptor.TYPE_EQUAL:
                // hash
                key = createPattern(name, value);
//                jedis.sadd(key, id);
                redisTemplate.boundSetOps(key).add(id);
                break;
            case FieldDescriptor.TYPE_MATCH:
                key = createPattern(name);
//                jedis.zadd(key, 0, value + ":" + id);
                redisTemplate.boundZSetOps(key).add(value + ":" + id,0);
                break;
            case FieldDescriptor.TYPE_RANGE:
                key = createPattern(name);
                double score = toDouble(value, 0);
//                jedis.zadd(key, score, id);
                redisTemplate.boundZSetOps(key).add(id,score);
                break;
            default:
                break;
            }
        });
    }

    public void deleteById(String id) {
        T old = selectById(id);
        if (old != null) {
            String idHashKey = createPattern();
//            jedis.hdel(idHashKey, id);
            redisTemplate.boundHashOps(idHashKey).delete(id);
            fieldDescriptorMap.forEach((name, field) -> {
                String key;
                String value = String.valueOf(getFieldValue(field.field, old));
                switch (field.getType()) {
                case FieldDescriptor.TYPE_EQUAL:
                    // hash
                    key = createPattern(name, value);
                    //jedis.srem(key, id);
                    redisTemplate.boundSetOps(key).remove(id);
                    break;
                case FieldDescriptor.TYPE_MATCH:
                    key = createPattern(name);
                    //jedis.zrem(key, value + ":" + id);
                    redisTemplate.boundZSetOps(key).remove(value + ":" + id);
                    break;
                case FieldDescriptor.TYPE_RANGE:
                    key = createPattern(name);
                    //jedis.zrem(key, id);
                    redisTemplate.boundZSetOps(key).remove(id);
                    break;
                default:
                    break;
                }
            });
        }
    }

    public void updateById(T obj) {
        String id = getIdValue(obj);
        T old = selectById(id);
        String pattern = createPattern();
        String json = convertToJson(obj);
//        Map<String, String> map = new HashMap<>();
//        map.put(id, json);
//        jedis.hmset(pattern, map);
        redisTemplate.boundHashOps(pattern).put(id,json);
        if (old != null) {
            try {
                Map<String, String> newVal = BeanUtils.describe(obj);
                Map<String, String> oldVal = BeanUtils.describe(old);
                Set<String> keys = new HashSet<>();
                keys.addAll(newVal.keySet());
                keys.addAll(oldVal.keySet());
                for (String k : keys) {
                    FieldDescriptor fd = fieldDescriptorMap.get(k);
                    if (fd == null) {
                        continue;
                    }
                    String nv = newVal.get(k);
                    String ov = oldVal.get(k);
                    if (StringUtils.equals(nv, ov)) {
                        continue;
                    }
                    if (FieldDescriptor.TYPE_EQUAL.equals(fd.getType())) {
                        String okey = createPattern(k, ov);
//                        jedis.srem(okey, id);
                        redisTemplate.boundSetOps(okey).remove(id);
                        String nkey = createPattern(k, nv);
//                        jedis.sadd(nkey, id);
                        redisTemplate.boundSetOps(nkey).add(id);
                    } else if (FieldDescriptor.TYPE_MATCH.equals(fd.getType())) {
                        String key = createPattern(k);
//                        jedis.zrem(key, ov + ":" + id);
//                        jedis.zadd(key, 0, nv + ":" + id);
                        redisTemplate.boundZSetOps(key).remove(ov + ":" + id);
                        redisTemplate.boundZSetOps(key).add(nv + ":" + id,0);
                    } else if (FieldDescriptor.TYPE_RANGE.equals(fd.getType())) {
                        String key = createPattern(k);
                        double score = toDouble(nv, 0);
//                        jedis.zadd(key, score, id);
                        redisTemplate.boundZSetOps(key).add(id,score);
                    }
                }
            } catch (Exception e) {
                asRuntime(e);
            }
        }
    }

    public T selectById(String id) {
        String pattern = createPattern();
        //List<String> list = jedis.hmget(pattern, id);
        String json = (String)redisTemplate.boundHashOps(pattern).get(id);
        if (json == null || json.length()==0 || StringUtils.isEmpty(json))
            return null;
        return convertToObject(json);
    }

    /**
     *
     * @param field
     * @param value
     * @return at least empty List (never return null)
     */
    public List<T> selectByEqField(String field, String... value) {
        if (idDescriptor.getName().equals(field)) {
//            List<String> jsons = jedis.hmget(createPattern(), value);
           BoundHashOperations<String,String,String> boundHashOperations = redisTemplate.boundHashOps(createPattern());
           List<String> jsons = boundHashOperations.multiGet(Arrays.asList(value));
            return convertToObjects(jsons);
        }
        FieldDescriptor fd = fieldDescriptorMap.get(field);
        if (fd == null || !FieldDescriptor.TYPE_EQUAL.equals(fd.getType()))
            return Collections.emptyList();
        String[] keys = new String[value.length];
        for (int i = 0; i < value.length; i++) {
            keys[i] = createPattern(field, value[i]);
        }
        //Set<String> sets = jedis.sunion(keys);
        Set<String> sets =  redisTemplate.opsForSet().union(keys[0], Arrays.asList(keys));
        if (sets.isEmpty()) {
            return Collections.emptyList();
        }
        //String[] ids = sets.toArray(new String[sets.size()]);
        //List<String> jsonList = jedis.hmget(createPattern(), ids);
        BoundHashOperations<String,String,String> boundHashOperations = redisTemplate.boundHashOps(createPattern());
        List<String> jsonList =boundHashOperations.multiGet(sets);
        return convertToObjects(jsonList);
    }

    public List<T> selectByMatchField(String field, String prefix, int offset, int count) {
        FieldDescriptor fd = fieldDescriptorMap.get(field);
        if (fd == null || !FieldDescriptor.TYPE_MATCH.equals(fd.getType()))
            return Collections.emptyList();
        String key = createPattern(field);
        //Set<String> strList = jedis.zrangeByLex(key, "[" + prefix, "(" + prefix + _LAST, offset, count);
        RedisZSetCommands.Range range = new RedisZSetCommands.Range();
        range.gte(prefix);
        RedisZSetCommands.Limit limit = new RedisZSetCommands.Limit();
        limit.offset(offset);
        limit.count(count);
        Set<String> strList = redisTemplate.boundZSetOps(key).rangeByLex(range,limit);
        if (strList.isEmpty()) {
            return Collections.emptyList();
        }
        String[] ids = new String[strList.size()];
        int i = 0;
        for (String str : strList) {
            ids[i++] = StringUtils.substringAfterLast(str, ":");
        }
        // TODO id sort ?
        //List<String> jsons = jedis.hmget(createPattern(), ids);
        BoundHashOperations<String,String,String> boundHashOperations = redisTemplate.boundHashOps(createPattern());
        List<String> jsons = boundHashOperations.multiGet(Arrays.asList(ids));
        return convertToObjects(jsons);
    }

    public List<T> selectByRangeField(String field, double min, double max, int offset, int count) {
        FieldDescriptor fd = fieldDescriptorMap.get(field);
        if (fd == null || !FieldDescriptor.TYPE_RANGE.equals(fd.getType()))
            return Collections.emptyList();
        String key = createPattern(field);
        //Set<String> sets = jedis.zrangeByScore(key, min, max, offset, count);
        Set<String> sets = redisTemplate.opsForZSet().rangeByScore(key, min, max, offset, count);
        if (sets.isEmpty())
            return Collections.emptyList();
        //String[] ids = sets.toArray(new String[sets.size()]);
        //List<String> jsonList = jedis.hmget(createPattern(), ids);
        BoundHashOperations<String,String,String> boundHashOperations = redisTemplate.boundHashOps(createPattern());
        List<String> jsonList = boundHashOperations.multiGet(sets);
        return convertToObjects(jsonList);

    }

    protected void asRuntime(Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        }
        throw new RuntimeException(throwable);
    }

    protected String getIdValue(Object obj) {
        return String.valueOf(getFieldValue(idDescriptor.field, obj));
    }

    protected String convertToJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected T convertToObject(String json) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (IOException e) {
            asRuntime(e);
        }
        return null;
    }

    protected List<T> convertToObjects(Collection<String> jsons) {
        List<T> list = new ArrayList<>();
        try {
            for (String json : jsons)
                list.add(objectMapper.readValue(json, clazz));
        } catch (IOException e) {
            asRuntime(e);
        }
        return list;
    }

    protected Object getFieldValue(Field f, Object obj) {
        f.setAccessible(true);
        try {
            return f.get(obj);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    protected double toDouble(String str, double def) {
        if (StringUtils.isEmpty(str))
            return def;
        return Double.parseDouble(str);
    }

    protected String createPattern(String... args) {
        StringBuilder sb = new StringBuilder();
        sb.append("hmall:cache:");
        sb.append('{').append(hashTag).append('}');
        sb.append(clazz.getSimpleName());
        for (String s : args) {
            sb.append(':').append(s);
        }
        return sb.toString();
    }

    public static FieldDescriptor createFieldDescriptor(Class clazz, String fieldName, String type) {
        try {
            Field f = clazz.getDeclaredField(fieldName);
            return new FieldDescriptor(f, type);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

}
