/*
 * Copyright Hand China Co.,Ltd.
 */

package com.hand.redis;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * @author shengyang.zhou@hand-china.com
 */
public class Staff {
    private Long id;

    private String name;

    private int age;

    private String dept;

    private String type;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getDept() {
        return dept;
    }

    public void setDept(String dept) {
        this.dept = dept;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
