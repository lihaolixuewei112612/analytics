package com.dtc.analytic.java.demo;

import com.sun.istack.Nullable;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Created on 2019-06-06
 *
 * @author :hao.li
 */
public class MessageWaterEmitter implements AssignerWithPunctuatedWatermarks<String> {
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
        if (lastElement != null && lastElement.contains(",")) {
            String[] parts = lastElement.split(",");
            return new Watermark(Long.parseLong(parts[0]));
        }
        return null;
    }

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        if (element != null && element.contains(",")) {
            String[] parts = element.split(",");
            return Long.parseLong(parts[0]);
        }
        return 0L;
    }
}
