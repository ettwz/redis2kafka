package com.hand.dao;

import com.hand.util.redis.Field.FieldDescriptor;
import com.hand.util.redis.dao.BaseDao;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

/**
 * Created by DongFan on 2016/11/16.
 */
@Service("promptDao")
public class PromptDao extends BaseDao{
    public PromptDao() {
        this.clazz = "Prompt:zh";
        this.hashTag = "prompt";
        this.idDescriptor = new FieldDescriptor("code");
    }
}
