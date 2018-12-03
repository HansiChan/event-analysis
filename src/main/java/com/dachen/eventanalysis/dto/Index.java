package com.dachen.eventanalysis.dto;

import java.util.LinkedList;
import java.util.List;


public enum  Index {

    NEW("人数", "people"),
    ACTIVE("次数", "times"),
    AUTHENTICATING("人均次数", "avgtimes"),
    AUTHENTICATED("活跃比", "active");

    private String name;
    private String index;

    // 构造方法
    Index(String name, String index) {
        this.name = name;
        this.index = index;
    }

    // 普通方法
    public static String getName(String index) {
        for (Index i : Index.values()) {
            if (i.getIndex().equals(index)) {
                return i.name;
            }
        }
        return null;
    }

    public static List<String> getNameList() {
        List<String> list = new LinkedList<>();
        for (Index i : Index.values()) {
            list.add(i.getName());
        }
        return list;
    }

    public static List<String> getInedxList() {
        List<String> list = new LinkedList<>();
        for (Index i : Index.values()) {
            list.add(i.getIndex());
        }
        return list;
    }

    // get set 方法
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }
}
