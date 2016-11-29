package com.hand.util.redis.Field;

/**
 * Created by DongFan on 2016/11/15.
 */
public class FieldDescriptor {
    String name;
    String type = FieldType.TYPE_EQUAL;

    public FieldDescriptor() {
    }

    public FieldDescriptor(String name) {
        this.name = name;
    }

    public FieldDescriptor(String name, String type) {
        this.name = name;
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
