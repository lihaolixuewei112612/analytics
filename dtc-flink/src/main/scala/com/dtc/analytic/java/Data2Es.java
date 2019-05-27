//package com.dtc.analytic.java;
//
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.java.tuple.Tuple5;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
//import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
//import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
//import org.apache.http.HttpHost;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.client.Requests;
//
//import java.util.*;
//
//public class Data2Es {
//    public static final String HOST = "10.3.6.7,10.3.6.12,10.3.6.16";// 服务器地址，本机
//    public static final int PORT = 9300; // http请求的端口是9200，客户端是9300
//    private static final String kafkaBrokers = "10.3.6.7:9092,10.3.6.12:9092,10.3.6.16:9092";
//    private static final String zkBrokers = "10.3.6.7:2181,10.3.6.12:2181,10.3.6.16:2181";
//    private static String topic = "test—dtc";
//    private static String groupId = "bigdata_dtc";
//
//    public static void main(String[] args) throws Exception {
//        System.out.println("===============》 flink任务开始  ==============》");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //设置kafka连接参数
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", kafkaBrokers);
//        properties.setProperty("zookeeper.connect", zkBrokers);
//        properties.setProperty("group.id", groupId);
//        //设置时间类型
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        //设置检查点时间间隔
//        env.enableCheckpointing(5000);
//        //创建kafak消费者，获取kafak中的数据
//        FlinkKafkaConsumer09<String> kafkaConsumer09 = new FlinkKafkaConsumer09<>(topic, new SimpleStringSchema(), properties);
//        kafkaConsumer09.setStartFromEarliest();
//        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer09);
//        kafkaData.print();
//        //解析kafka数据流 转化成固定格式数据流
//        DataStream<Tuple5<Long, Long, Long, String, Long>> userData = kafkaData.map(new MapFunction<String, Tuple5<Long, Long, Long, String, Long>>() {
//            @Override
//            public Tuple5<Long, Long, Long, String, Long> map(String s) throws Exception {
//                Tuple5<Long, Long, Long, String, Long> userInfo = null;
//                String[] split = s.split(",");
//                if (split.length != 5) {
//                    System.out.println(s);
//                } else {
//                    long userID = Long.parseLong(split[0]);
//                    long itemId = Long.parseLong(split[1]);
//                    long categoryId = Long.parseLong(split[2]);
//                    String behavior = split[3];
//                    long timestamp = Long.parseLong(split[4]);
//                    userInfo = new Tuple5<>(userID, itemId, categoryId, behavior, timestamp);
//                }
//                return userInfo;
//            }
//        });
//        List<HttpHost> httpHosts = new ArrayList<>();
//        httpHosts.add(new HttpHost("ip", 9200, "http"));
//        ElasticsearchSink.Builder<Tuple5<Long, Long, Long, String, Long>> esSinkBuilder = new ElasticsearchSink.Builder<>(
//                httpHosts,
//                new ElasticsearchSinkFunction<Tuple5<Long, Long, Long, String, Long>>() {
//                    public IndexRequest createIndexRequest(Tuple5<Long, Long, Long, String, Long> element) {
//                        Map<String, String> json = new HashMap<>();
//                        json.put("userid", element.f0.toString());
//                        json.put("itemid", element.f1.toString());
//                        json.put("categoryid", element.f2.toString());
//                        json.put("behavior", element.f3);
//                        json.put("timestamp", element.f4.toString());
//                        return Requests.indexRequest()
//                                .index("flink")
//                                .type("user")
//                                .source(json);
//                    }
//
//                    @Override
//                    public void process(Tuple5<Long, Long, Long, String, Long> element, RuntimeContext ctx, RequestIndexer indexer) {
//                        indexer.add(createIndexRequest(element));
//                    }
//                }
//        );
//        /*     必须设置flush参数     */
//        //刷新前缓冲的最大动作量
//        esSinkBuilder.setBulkFlushMaxActions(1);
//        //刷新前缓冲区的最大数据大小（以MB为单位）
//        esSinkBuilder.setBulkFlushMaxSizeMb(500);
//        //论缓冲操作的数量或大小如何都要刷新的时间间隔
//        esSinkBuilder.setBulkFlushInterval(5000);
//        userData.addSink(esSinkBuilder.build());
//        env.execute("data2es");
//    }
//}
//
//
