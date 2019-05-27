//package com.dtc.analytic.java;
//
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.time.Time;
//import org.apache.flink.api.java.tuple.Tuple5;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
//import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
//import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
//import org.apache.http.HttpHost;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.client.Requests;
//
//import java.net.InetAddress;
//import java.net.InetSocketAddress;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//
//public class test {
//    public static final String HOST = "10.3.6.7,10.3.6.12,10.3.6.16";// 服务器地址，本机
//    public static final int PORT = 9200; // http请求的端口是9200，客户端是9300
//
//    public static void main(String[] args) throws Exception {
//        System.out.println("===============》 flink任务开始  ==============》");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Map properties = new HashMap();
//        properties.put("bootstrap.servers", "10.3.6.7:9092,10.3.6.12:9092,10.3.6.16:9092");
//        properties.put("group.id", "console-consumer-58320");
//        properties.put("enable.auto.commit", "true");
//        properties.put("auto.commit.interval.ms", "1000");
//        properties.put("auto.offset.reset", "earliest");
//        properties.put("session.timeout.ms", "30000");
//        properties.put("zookeeper.connect", "10.3.6.7:2181,10.3.6.12:2181,10.3.6.16:2181");
//        properties.put("topic", "test_dtc_one");
////        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
////        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//        //es configure
//        properties.put("cluster.name", "common_test");
////该配置表示批量写入ES时的记录条数
//        properties.put("bulk.flush.max.actions", "1");
//        //1、用来表示是否开启重试机制
//        properties.put("bulk.flush.backoff.enable", "true");
////2、重试策略，又可以分为以下两种类型
//        //a、指数型，表示多次重试之间的时间间隔按照指数方式进行增长。eg:2 -> 4 -> 8 ...
//        properties.put("bulk.flush.backoff.type", "EXPONENTIAL");
//        //b、常数型，表示多次重试之间的时间间隔为固定常数。eg:2 -> 2 -> 2 ...
//        properties.put("bulk.flush.backoff.type", "CONSTANT");
////3、进行重试的时间间隔。对于指数型则表示起始的基数
//        properties.put("bulk.flush.backoff.delay", "2");
////4、失败重试的次数
//        properties.put("bulk.flush.backoff.retries", "3");
//        List<InetSocketAddress> transportAddresses = new ArrayList<>();
//        for (String host : HOST.split(",")) {
//            InetSocketAddress inetSocketAddress = new InetSocketAddress(InetAddress.getByName(host), PORT);
//            transportAddresses.add(inetSocketAddress);
//        }
//        env.setParallelism(1);  //设置并行度
//        env.getConfig().setAutoWatermarkInterval(9000);//每9秒发出一个watermark
//        // parse user parameters
//        ParameterTool parameterTool = ParameterTool.fromMap(properties);
//        env.getConfig().disableSysoutLogging();
//        //job发生故障时的重启策略。重试4次，每次间隔10s
//        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, Time.of(10, TimeUnit.SECONDS)));
//        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
//        env.getConfig().setGlobalJobParameters(parameterTool);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        FlinkKafkaConsumer09<String> consumer09 = new FlinkKafkaConsumer09<String>(
//                parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties());
//        DataStream<String> messageStream = env
//                .addSource(consumer09);
//        messageStream.print();
//        // print() will write the contents of the stream to the TaskManager's standard out stream
//        // the rebelance call is causing a repartitioning of the data so that all machines
//        // see the messages (for example in cases when "num kafka partitions" < "num flink operators"
////        messageStream.rebalance().map(new MapFunction<String, String>() {
////            private static final long serialVersionUID = 1L;
////            @Override
////            public String map(String value) throws Exception {
////                //水印时间在这里定义
////                return value;
////            }
////        });
////        System.out.println("Starting to consumer data...");
//
//        //解析kafka数据流 转化成固定格式数据流
//        DataStream<Tuple5<Long, Long, Long, String, Long>> userData = messageStream.map(new MapFunction<String, Tuple5<Long, Long, Long, String, Long>>() {
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
//        for (String host : HOST.split(",")) {
//            HttpHost httpHost = new HttpHost(host, PORT);
//            httpHosts.add(httpHost);
//        }
//        ElasticsearchSink.Builder<Tuple5<Long, Long, Long, String, Long>> esSinkBuilder = new ElasticsearchSink.Builder<>(
//                httpHosts,
//                new ElasticsearchSinkFunction<Tuple5<Long, Long, Long, String, Long>>() {
//                    public IndexRequest createIndexRequest(Tuple5<Long, Long, Long, String, Long> element) {
//                        Map<String, String> json = new HashMap<>();
//                        json.put("a", element.f0.toString());
//                        json.put("b", element.f1.toString());
//                        json.put("c", element.f2.toString());
//                        json.put("d", element.f3);
//                        json.put("e", element.f4.toString());
//                        return Requests.indexRequest()
//                                .index("flink")
//                                .type("test")
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
//
//}
