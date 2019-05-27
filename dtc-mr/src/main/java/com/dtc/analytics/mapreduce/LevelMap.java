package com.dtc.analytics.mapreduce;

import org.apache.hadoop.io.Text;

public enum LevelMap {

    INFO("info", 1),
    DEBUG("debug", 2),
    NOTICE("notice", 3),
    WARNING("warning", 4),
    ERR("err", 5),
    CRIT("crit", 6),
    ALTER("alter", 7),
    EMERG("emerg", 8);


    private String name;
    private int index;

    LevelMap(String k, int v) {
        name = k;
        index = v;
    }

    // 普通方法
    public static int getIndex(String name) {
        for (LevelMap c : LevelMap.values()) {
            if (c.getName() == name) {
                return c.getIndex();
            }
        }
        return 1;
    }

    // 普通方法
    public static String getName(int index) {
        for (LevelMap c : LevelMap.values()) {
            if (c.getIndex() == index) {
                return c.getName();
            }
        }
        return null;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
