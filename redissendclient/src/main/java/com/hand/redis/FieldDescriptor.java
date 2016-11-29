/*
 * Copyright Hand China Co.,Ltd.
 */

package com.hand.redis;

import java.lang.reflect.Field;

/**
 * @author shengyang.zhou@hand-china.com
 */
public class FieldDescriptor {
    public static final String TYPE_EQUAL = "equal";
    public static final String TYPE_MATCH = "match";
    public static final String TYPE_RANGE = "range";
    String name;
    String type = TYPE_EQUAL;

    Field field;

    public FieldDescriptor() {

    }

    public FieldDescriptor(Field f, String type) {
        this.field = f;
        this.name = f.getName();
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
