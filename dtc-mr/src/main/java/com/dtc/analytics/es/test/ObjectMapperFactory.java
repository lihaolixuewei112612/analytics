package com.dtc.analytics.es.test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @Author :lihao
 * @Date :Created in 10:25 2019-04-28
 */
public class ObjectMapperFactory {
    private ObjectMapperFactory() {
    }

    public static ObjectMapper getDefaultMapper() {
        return Holder.instance;
    }

    private static class Holder {
        private static ObjectMapper instance = new ObjectMapper();
    }

}
