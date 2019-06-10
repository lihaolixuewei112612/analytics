//package com.dtc.analytic.java.demo;
//
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
//import org.apache.flink.util.Collector;
//
//import java.util.Properties;
//
///**
// * Created on 2019-06-06
// *
// * @author :hao.li
// */
//public class KafkaMessageStreaming {
//
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000); // 非常关键，一定要设置启动检查点！！
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", "10.3.6.7:9092,10.3.6.12:9092,10.3.6.16:9092");
//        props.setProperty("group.id", "bigdata_test");
//
//        FlinkKafkaConsumer09<String> consumer =
//                new FlinkKafkaConsumer09<>("connect-test", new SimpleStringSchema(), props);
//
//        consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());
//
//        DataStream<Tuple2<String, Long>> keyedStream = env
//                .addSource(consumer)
//                .flatMap(new MessageSplitter())
//                .keyBy(0)
//                .timeWindow(Time.seconds(10))
//                .apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow>() {
//                    @Override
//                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {
//                        long sum = 0L;
//                        int count = 0;
//                        for (Tuple2<String, Long> record: input) {
//                            sum += record.f1;
//                            count++;
//                        }
//                        Tuple2<String, Long> result = input.iterator().next();
//                        result.f1 = sum / count;
//                        out.collect(result);
//                    }
//                });
//
//        keyedStream.writeAsText("conf/test.txt");
//        env.execute("Flink-Kafka demo");
//    }
//}
