//package com.dtc.analytic.java; /**
// * Copyright @ 2018
// * All right reserved.
// *
// * @author Li Hao
// * @since 2019/2/18  23:23
// */
//
//import org.apache.flink.api.common.functions.FoldFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
//import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
//import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
//
//public class KafkaProducer {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
//        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
//                .keyBy(new KeySelector<WikipediaEditEvent, String>() {
//                    @Override
//                    public String getKey(WikipediaEditEvent event) {
//                        return event.getUser();
//                    }
//                });
//
//        DataStream<Tuple2<String, Long>> result = keyedEdits
//                .timeWindow(Time.seconds(5))
//                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
//                        acc.f0 = event.getUser();
//                        acc.f1 += event.getByteDiff();
//                        return acc;
//                    }
//                });
//        result.print();
//
//        result.map(new MapFunction<Tuple2<String, Long>, String>() {
//                    @Override
//                    public String map(Tuple2<String, Long> tuple) {
//                        return tuple.toString();
//                    }
//                })
//                .addSink(new FlinkKafkaProducer09<>("10.3.6.7:9092,10.3.6.12:9092,10.3.6.16:9092", "test_dtc", new SimpleStringSchema()));
//        see.execute();
//    }
//}
