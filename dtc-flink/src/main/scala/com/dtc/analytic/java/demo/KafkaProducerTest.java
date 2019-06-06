package com.dtc.analytic.java.demo;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Created on 2019-06-06
 *
 * @author :hao.li
 */
public class KafkaProducerTest {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.3.6.7:9092,10.3.6.12:9092,10.3.6.16:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        int totalMessageCount = 10000;
        for (int i = 0; i < totalMessageCount; i++) {
            String value = String.format("%d,%s,%d", System.currentTimeMillis(), "machine-1", currentMemSize());
            producer.send(new ProducerRecord<String,String>("connect-test", value), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println("Failed to send message with exception " + exception);
                    }
                }
            });
            Thread.sleep(1000L);
        }
        producer.close();
    }

    private static long currentMemSize() {
        return MemoryUsageExtrator.currentFreeMemorySizeInBytes();
    }
}
