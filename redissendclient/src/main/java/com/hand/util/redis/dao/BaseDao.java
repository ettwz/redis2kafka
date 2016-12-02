package com.hand.util.redis.dao;

import java.util.*;

import javax.annotation.Resource;

import com.hand.util.redis.Field.PubClient;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.hand.util.json.JsonMapper;
import com.hand.util.redis.Field.FieldDescriptor;
import com.hand.util.redis.Field.FieldType;

/**
 * Created by DongFan on 2016/11/15.
 */
public abstract class BaseDao {

    @Resource(name = "pubClient")
    protected PubClient pubClient;

    @Resource(name = "stringRedisTemplate")
    protected StringRedisTemplate redisTemplate;

    @Resource(name = "jsonMapper")
    protected JsonMapper jsonMapper;

    final String channel = "test";
    protected String mime = "ooooooossssss";

    protected String hashTag = "";
    protected String clazz;
    protected FieldDescriptor idDescriptor;
    Map<String, FieldDescriptor> fieldDescriptorMap = new HashMap<>();

    private static Logger logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);

    public void addFieldDescriptor(FieldDescriptor fieldDescriptor) {
        fieldDescriptorMap.put(fieldDescriptor.getName(), fieldDescriptor);
    }

    public static FieldDescriptor createFieldDescriptor(String fieldName, String type) {
        return new FieldDescriptor(fieldName, type);
    }

    /**
     * 返回总数
     *
     * @return
     */
    public int count() {
        String key = createPattern();
        //return jedis.hlen(key).intValue();
        int num = redisTemplate.boundHashOps(key).size().intValue();
        logger.debug("select from " + createPattern() + " records numbers is：" + num);
        return num;
    }

    /**
     * 添加一条记录
     *
     * @param map
     */
    public void add(Map<String, ?> map) {
        String id = getIdValue(map);
        String pattern = createPattern();
        String json = jsonMapper.convertToJson(map);

        //将新增的数据插入到redis Hash中
        redisTemplate.opsForHash().put(pattern, id, json);
        pubClient.pub(channel, "add|"+map);

        logger.debug("insert into " + pattern + "1 record");
//this.hashTag
        //遍历对象需要进行搜索的字段
        fieldDescriptorMap.forEach((name, field) -> {
            String key;
            Object fieldValue = getFieldValue(map, field.getName());
            String value = null;
            if (fieldValue != null) {
                value = String.valueOf(fieldValue);
            }
            if (value != null && fieldValue != null) {
                switch (field.getType()) {
                    case FieldType.TYPE_EQUAL:
                        key = createPattern(name, value);
                        redisTemplate.boundSetOps(key).add(id);
//                        pubClient.pub(channel, "add|"+id);
                        logger.debug("insert into " + key + "1 record" + idDescriptor.getName() + "is：" + id);
                        break;
                    case FieldType.TYPE_MATCH:
                        key = createPattern(name);
                        redisTemplate.boundZSetOps(key).add(value + ":" + id, 0);
                        logger.debug("insert into " + key + "1 record，" + idDescriptor.getName() + "is：" + id);
                        break;
                    //当字段为范围搜索时，需要将id插入到字段对应的ZSET中，以id为value,以字段值为score
                    case FieldType.TYPE_RANGE:
                        key = createPattern(name);
                        double score = toDouble(value, 0);
                        redisTemplate.boundZSetOps(key).add(id, score);
                        logger.debug("insert into " + key + "1 record，" + idDescriptor.getName() + "is：" + id);
                        break;
                    default:
                        break;
                }
            }
        });
    }

    /**
     * 根据id搜索对应的对象
     *
     * @param id
     * @return
     */
    public Map<String, ?> select(String id) {
        String pattern = createPattern();
        String json = (String) redisTemplate.boundHashOps(pattern).get(id);
        if (json == null || json.length() == 0 || StringUtils.isEmpty(json)) {
            logger.debug("select from " + pattern + " by id:" + id + "no record");
            return null;
        }
        logger.debug("select from " + pattern + " by id:" + id + "1 record");
        return jsonMapper.convertToMap(json);
    }

    /**
     * 删除一条记录
     *
     * @param id
     */
    public void delete(String id) {
        Map<String, ?> old = select(id);
        String idHashKey = createPattern();
        if (old != null) {
            //根据id删除对应redis Hash中的对象
            redisTemplate.boundHashOps(idHashKey).delete(id);
            pubClient.pub(channel, "del|"+id);
            logger.debug("delete from " + idHashKey + " by id:" + id + ",delete 1 record");

            //遍历对象所需分类查询的字段，将其对应的id删除
            fieldDescriptorMap.forEach((name, field) -> {
                String key;
                Object fieldValue = getFieldValue(old, field.getName());
                String value = null;
                if (fieldValue != null) {
                    value = String.valueOf(fieldValue);
                }
                if (fieldValue != null && value != null) {
                    switch (field.getType()) {
                        case FieldType.TYPE_EQUAL:
                            key = createPattern(name, value);
                            redisTemplate.boundSetOps(key).remove(id);
                            logger.debug("delete from " + key + " by " + idHashKey + " id: " + id + ",delete 1 record");
                            break;
                        case FieldType.TYPE_MATCH:
                            key = createPattern(name);
                            redisTemplate.boundZSetOps(key).remove(value + ":" + id);
                            logger.debug("delete from " + key + " by " + idHashKey + " id: " + id + "删除一条记录");
                            break;
                        case FieldType.TYPE_RANGE:
                            key = createPattern(name);
                            redisTemplate.boundZSetOps(key).remove(id);
                            logger.debug("delete from " + key + " by " + idHashKey + " id: " + id + "删除一条记录");
                            break;
                        default:
                            break;
                    }
                }
            });
        } else {
            logger.debug("delete from " + idHashKey + " by id:" + id + "with none record,so not do delete");
        }
    }

    /**
     * 更新一条记录
     *
     * @param map
     */
    public void update(Map<String, ?> map) {
        String id = getIdValue(map);
        Map<String, ?> old = select(id);
        String pattern = createPattern();
        String json = jsonMapper.convertToJson(map);

        //根据id更新redis Hash对象
        redisTemplate.boundHashOps(pattern).put(id, json);
        pubClient.pub(channel, "up|"+map);
        //clazz oracle对接也许需要
        logger.debug("update from " + pattern + " by id:" + id + " ,update 1 record");

        //当操作确实为更新操作时，删除原来旧字段所在的索引Set对应的id的值，将更新后的字段与id插入新的索引Set中
        if (old != null) {
            try {
                Set<String> keys = new HashSet<>();
                keys.addAll(map.keySet());
                keys.addAll(old.keySet());
                for (String k : keys) {
                    FieldDescriptor fd = fieldDescriptorMap.get(k);
                    if (fd == null) {
                        continue;
                    }
                    Object nv = map.get(k);
                    Object ov = old.get(k);
                    //当新数据和旧数据都为空时，直接跳过
                    if (nv == null && ov == null) {
                        continue;
                    }
                    //当新数据和旧数据都不为空，且相等时跳过
                    if (nv != null && ov != null && StringUtils.equals(nv.toString(), ov.toString())) {
                        continue;
                    }
                    if (FieldType.TYPE_EQUAL.equals(fd.getType())) {
                        if (ov != null) {
                            String okey = createPattern(k, String.valueOf(ov));
                            redisTemplate.boundSetOps(okey).remove(id);
                            logger.debug("delete from " + okey + " by " + pattern + " id: " + id + ",delete 1 record");
                        }
                        if (nv != null) {
                            String nkey = createPattern(k, String.valueOf(nv));
                            redisTemplate.boundSetOps(nkey).add(id);
                            logger.debug("insert into " + nkey + " by " + pattern + " id: " + id + ",insert 1 record");
                        }
                    } else if (FieldType.TYPE_MATCH.equals(fd.getType())) {
                        String key = createPattern(k);
                        if (ov != null) {
                            redisTemplate.boundZSetOps(key).remove(ov + ":" + id);
                            logger.debug("delete from " + key + " by " + pattern + " id: " + id + ",delete 1 record");
                        }
                        if (nv != null) {
                            redisTemplate.boundZSetOps(key).add(nv + ":" + id, 0);
                            logger.debug("insert into " + key + " by " + pattern + " id: " + id + ",insert 1 record");
                        }
                    } else if (FieldType.TYPE_RANGE.equals(fd.getType())) {
                        String key = createPattern(k);
                        //若nv不为空更新数据
                        if (nv != null) {
                            double score = toDouble(String.valueOf(nv), 0);
                            redisTemplate.boundZSetOps(key).add(id, score);
                            logger.debug("update into " + key + " by " + pattern + " id: " + id + ",update 1 record");
                        }
                        //若ov为不为空nv为空删除记录
                        else if (ov != null && nv == null) {
                            redisTemplate.boundZSetOps(key).remove(id);
                            logger.debug("delete from " + key + " by " + pattern + " id: " + id + ",delete 1 record");
                        }
                    }
                }
            } catch (Exception e) {
            	jsonMapper.asRuntime(e);
            }
        }
    }

    /**
     * @param field
     * @param value
     * @return at least empty List (never return null)
     * 根据索引字段，进行相等匹配搜索
     */
    public List<Map<String, ?>> selectByEqField(String field, String... value) {
        if (idDescriptor.getName().equals(field)) {
            BoundHashOperations<String, String, String> boundHashOperations = redisTemplate.boundHashOps(createPattern());
            List<String> jsons = boundHashOperations.multiGet(Arrays.asList(value));
            logger.debug("select from " + createPattern() + " by " + value + " with equal search,select records numbser is " + jsons.size());
            return jsonMapper.convertToList(jsons);
        }
        FieldDescriptor fd = fieldDescriptorMap.get(field);
        if (fd == null || !FieldType.TYPE_EQUAL.equals(fd.getType())) {
            logger.debug("select from " + createPattern() + " by " + value + " with equal search，due to" + field + "not a equal field，with no record");
            return Collections.emptyList();
        }
        String[] keys = new String[value.length];
        for (int i = 0; i < value.length; i++) {
            keys[i] = createPattern(field, value[i]);
        }
        Set<String> sets = redisTemplate.opsForSet().union(keys[0], Arrays.asList(keys));
        if (sets.isEmpty()) {
            logger.debug("select from " + createPattern() + " by " + value + " with equal search,select records numbser is " + 0);
            return Collections.emptyList();
        }
        BoundHashOperations<String, String, String> boundHashOperations = redisTemplate.boundHashOps(createPattern());
        List<String> jsonList = boundHashOperations.multiGet(sets);
        logger.debug("select from " + createPattern() + " by " + value + " with equal search,select records numbser is " + jsonList.size());
        return jsonMapper.convertToList(jsonList);
    }


    //根据多个Eq属性取交集
    public List<Map<String, ?>> selectByMutilEqField(Map<String, String> fieldsMap) {
        String hashKey = createPattern();
        if (fieldsMap == null || fieldsMap.size() <= 0) {
            logger.debug("select from " + hashKey + " by " + fieldsMap + " with intersect equal search,with no result");
            return Collections.emptyList();
        }
        List<String> keys = new ArrayList<>();
        Set<String> mapKeys = fieldsMap.keySet();
        for (String mapKey : mapKeys) {
            String mapValue = fieldsMap.get(mapKey);
            String key = createPattern(mapKey, mapValue);
            keys.add(key);
        }
        Set<String> keysSet = redisTemplate.opsForSet().intersect(keys.get(0), keys);
        if (keysSet == null || keysSet.size() <= 0) {
            logger.debug("select from " + hashKey + " by " + fieldsMap + " with intersect equal search,with no result");
            return Collections.emptyList();
        }
        BoundHashOperations<String, String, String> hashOperations = redisTemplate.boundHashOps(hashKey);
        List<String> jsons = hashOperations.multiGet(keysSet);
        logger.debug("select from " + hashKey + " by " + fieldsMap + " with intersect equal search,select records number is " + jsons.size());
        return jsonMapper.convertToList(jsons);
    }

    /**
     * @param field
     * @param prefix 匹配模式
     * @return at least empty List (never return null)
     * 根据索引字段，进行模糊匹配搜索
     * @Param offset 搜索结果偏移量
     * @Param count  搜索结果取值长度
     */
    public List<Map<String, ?>> selectByMatchField(String field, String prefix, int offset, int count) {
        FieldDescriptor fd = fieldDescriptorMap.get(field);
        String key = createPattern(field);
        if (fd == null || !FieldType.TYPE_MATCH.equals(fd.getType())) {
            logger.debug("select from " + createPattern() + " by " + prefix + " with equal search，due to" + field + "not a match field，with no record");
            return Collections.emptyList();
        }
        RedisZSetCommands.Range range = new RedisZSetCommands.Range();
        range.gte(prefix);
        RedisZSetCommands.Limit limit = new RedisZSetCommands.Limit();
        limit.offset(offset);
        limit.count(count);
        Set<String> strList = redisTemplate.boundZSetOps(key).rangeByLex(range, limit);
        if (strList.isEmpty()) {
            logger.debug("select from " + key + " by " + prefix + " with match search,select records numbser is " + 0);
            return Collections.emptyList();
        }
        String[] ids = new String[strList.size()];
        int i = 0;
        for (String str : strList) {
            ids[i++] = StringUtils.substringAfterLast(str, ":");
        }
        BoundHashOperations<String, String, String> boundHashOperations = redisTemplate.boundHashOps(createPattern());
        List<String> jsons = boundHashOperations.multiGet(Arrays.asList(ids));
        logger.debug("select from " + key + " by " + prefix + " with match search,select records numbser is " + jsons.size());
        return jsonMapper.convertToList(jsons);
    }

    /**
     * @param field
     * @return at least empty List (never return null)
     * 根据索引字段，进行范围匹配搜索
     * @Param min 范围最小值
     * @Param max  范围最大值
     * @Param offset 搜索结果偏移量
     * @Param count  搜索结果取值长度
     */
    public List<Map<String, ?>> selectByRangeField(String field, double min, double max, int offset, int count) {
        FieldDescriptor fd = fieldDescriptorMap.get(field);
        if (fd == null || !FieldType.TYPE_RANGE.equals(fd.getType())) {
            logger.debug("select from " + createPattern() + " by " + min + " to " + max + " with equal search，due to" + field + "not a range field，with no record");
            return Collections.emptyList();
        }
        String key = createPattern(field);
        Set<String> sets = redisTemplate.opsForZSet().rangeByScore(key, min, max, offset, count);
        if (sets.isEmpty()) {
            logger.debug("select from " + key + " by " + min + " to " + max + " with range search,select records numbser is " + 0);
            return Collections.emptyList();
        }
        BoundHashOperations<String, String, String> boundHashOperations = redisTemplate.boundHashOps(createPattern());
        List<String> jsonList = boundHashOperations.multiGet(sets);
        logger.debug("select from " + key + " by " + min + " to " + max + " with range search,select records numbser is " + jsonList.size());
        return jsonMapper.convertToList(jsonList);
    }

    //倒序获取ByRange
    public List<Map<String, ?>> selectByRevRange(String field, double min, double max, int offset, int count) {
        FieldDescriptor fieldDescriptor = fieldDescriptorMap.get(field);
        if (fieldDescriptor == null || !fieldDescriptor.getType().equals(FieldType.TYPE_RANGE)) {
            logger.debug("select from " + createPattern() + " by " + min + " to " + max + " with equal search，due to" + field + "not a range field，with no record");
            return Collections.emptyList();
        }
        String key = createPattern(field);
        Set<String> ids = redisTemplate.opsForZSet().reverseRangeByScore(key, min, max, offset, count);
        if (ids == null || ids.size() <= 0) {
            logger.debug("select from " + key + " by " + min + " to " + max + " with range search,select records numbser is " + 0);
            return Collections.emptyList();
        }
        BoundHashOperations<String, String, String> hashOperations = redisTemplate.boundHashOps(createPattern());
        List<String> jsons = hashOperations.multiGet(ids);
        logger.debug("select from " + key + " by " + min + " to " + max + " with range search,select records numbser is " + jsons.size());
        return jsonMapper.convertToList(jsons);
    }

    public List<Map<String, ?>> selectAllByRangeField(String field, double min, double max) {
        FieldDescriptor fd = fieldDescriptorMap.get(field);
        if (fd == null || !FieldType.TYPE_RANGE.equals(fd.getType())) {
            logger.debug("select from " + createPattern() + " by " + min + " to " + max + " with equal search，due to" + field + "not a range field，with no record");
            return Collections.emptyList();
        }
        String key = createPattern(field);
        Set<String> sets = redisTemplate.boundZSetOps(key).rangeByScore(min, max);
        if (sets.isEmpty()) {
            logger.debug("select from " + key + " by " + min + " to " + max + " with range search,select records numbser is " + 0);
            return Collections.emptyList();
        }
        BoundHashOperations<String, String, String> boundHashOperations = redisTemplate.boundHashOps(createPattern());
        List<String> jsonList = boundHashOperations.multiGet(sets);
        logger.debug("select from " + key + " by " + min + " to " + max + " with range search,select records numbser is " + jsonList.size());
        return jsonMapper.convertToList(jsonList);
    }

    public List<Map<String, ?>> selectAllByRevRangeField(String field, double min, double max) {
        FieldDescriptor fd = fieldDescriptorMap.get(field);
        if (fd == null || !FieldType.TYPE_RANGE.equals(fd.getType())) {
            logger.debug("select from " + createPattern() + " by " + min + " to " + max + " with equal search，due to" + field + "not a range field，with no record");
            return Collections.emptyList();
        }
        String key = createPattern(field);
        Set<String> sets = redisTemplate.boundZSetOps(key).reverseRangeByScore(min, max);
        if (sets.isEmpty()) {
            logger.debug("select from " + key + " by " + min + " to " + max + " with range search,select records numbser is " + 0);
            return Collections.emptyList();
        }
        BoundHashOperations<String, String, String> boundHashOperations = redisTemplate.boundHashOps(createPattern());
        List<String> jsonList = boundHashOperations.multiGet(sets);
        logger.debug("select from " + key + " by " + min + " to " + max + " with range search,select records numbser is " + jsonList.size());
        return jsonMapper.convertToList(jsonList);
    }


    protected String getIdValue(Map<String, ?> map) {
        return String.valueOf(map.get(idDescriptor.getName()));
    }

    protected Object getFieldValue(Map<String, ?> map, String f) {
        if (map.containsKey(f)) {
            return map.get(f);
        } else {
            return null;
        }
    }

    //获取所有的值
    public List<Map<String,?>> selectAll(){
        BoundHashOperations<String,String,String> hashOperations = redisTemplate.boundHashOps(createPattern());
        List<String> jsons = hashOperations.values();
        return jsonMapper.convertToList(jsons);
    }

    protected String createPattern(String... args) {
        StringBuilder sb = new StringBuilder();
        sb.append("hmall:cache:");
        sb.append('{').append(hashTag).append('}');
        sb.append(clazz);
        for (String s : args) {
            sb.append(':').append(s);
        }
        return sb.toString();
    }

    protected double toDouble(String str, double def) {
        if (StringUtils.isEmpty(str))
            return def;
        return Double.parseDouble(str);
    }
}
