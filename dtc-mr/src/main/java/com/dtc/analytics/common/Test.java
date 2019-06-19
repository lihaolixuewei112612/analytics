package com.dtc.analytics.common;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

/**
 * Created on 2019-06-19
 *
 * @author :hao.li
 */
public class Test {
    static class Student {
        public String name;
        public long no;
        public Map<String,String> map;

        public Student(String name, long no,Map<String,String> map) {
            this.name = name;
            this.no = no;
            this.map = map;
        }
    }

    public static void main(String[] args) {
        Map<String,String> map = new HashMap<String,String>();
        map.put("1","2");
        map.put("3","4");
        String s1 = new Gson().toJson(new Student("zhangsan", 110,map));
        System.out.println(s1);
    }
}

