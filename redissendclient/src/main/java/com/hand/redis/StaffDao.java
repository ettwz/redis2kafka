package com.hand.redis;

import com.hand.util.redis.Field.FieldType;
import com.hand.util.redis.dao.BaseDao;
import org.springframework.stereotype.Service;

/**
 * Created by Hand on 2016/11/15.
 */
@Service
public class StaffDao extends BaseDao {
    public StaffDao() {
        super();
        this.clazz = "Staff";
        this.hashTag = "staff";
        idDescriptor = createFieldDescriptor("id", FieldType.TYPE_EQUAL);
        addFieldDescriptor(createFieldDescriptor("dept", FieldType.TYPE_EQUAL));
        addFieldDescriptor(createFieldDescriptor("type", FieldType.TYPE_EQUAL));
        addFieldDescriptor(createFieldDescriptor("name", FieldType.TYPE_MATCH));
        addFieldDescriptor(createFieldDescriptor("age", FieldType.TYPE_RANGE));
    }
}
