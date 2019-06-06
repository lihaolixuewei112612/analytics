package com.dtc.analytic.java;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.util.ExceptionUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Copyright @ 2018
 * All right reserved.
 *
 * @author Li Hao
 * @since 2019/2/19  9:09
 */
public class ReadFromKafka {
    public static final String HOST = "10.3.6.7,10.3.6.12,10.3.6.16";// 服务器地址，本机
    public static final int PORT = 9300; // http请求的端口是9200，客户端是9300

    public static void main(String[] args) throws Exception {

// create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map properties= new HashMap();
        properties.put("bootstrap.servers", "10.3.6.7:9092,10.3.6.12:9092,10.3.6.16:9092");
        properties.put("group.id", "bigdata—test");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
//        properties.put("zookeeper.connect","10.3.6.7:2181,10.3.6.12:2181,10.3.6.16:2181");
        properties.put("topic", "connect-test");
//        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

//        //es configure
//        properties.put("cluster.name", "common_test");
////该配置表示批量写入ES时的记录条数
//        properties.put("bulk.flush.max.actions", "1");
        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        for (String host : HOST.split(",")) {
            InetSocketAddress inetSocketAddress = new InetSocketAddress(InetAddress.getByName(host), PORT);
            transportAddresses.add(inetSocketAddress);
        }
        env.setParallelism(1);  //设置并行度
        env.getConfig().setAutoWatermarkInterval(9000);//每9秒发出一个watermark
        // parse user parameters
        ParameterTool parameterTool = ParameterTool.fromMap(properties);
        env.getConfig().disableSysoutLogging();
        //job发生故障时的重启策略。重试4次，每次间隔10s
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, Time.of(10, TimeUnit.SECONDS)));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        System.out.println(parameterTool.getRequired("topic"));


        FlinkKafkaConsumer09<String> consumer09 = new FlinkKafkaConsumer09<String>(
                parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties());
        DataStream<String> messageStream = env
                .addSource(consumer09);
        messageStream.print();

        // print() will write the contents of the stream to the TaskManager's standard out stream
        // the rebelance call is causing a repartitioning of the data so that all machines
        // see the messages (for example in cases when "num kafka partitions" < "num flink operators"
//        messageStream.rebalance().map(new MapFunction<String, String>() {
//            private static final long serialVersionUID = 1L;
//            @Override
//            public String map(String value) throws Exception {
//                //水印时间在这里定义
//                return value;
//            }
//        });
//        System.out.println("Starting to consumer data...");
//        messageStream.print();
//        messageStream.addSink(new ElasticsearchSink<>(properties, transportAddresses, new ElasticsearchSinkFunction<String>() {
//            public IndexRequest createIndexRequest(String element) {
//                Map<String, String> json = new HashMap<>();
//                //将需要写入ES的字段依次添加到Map当中
//                json.put("data", element);
//
//                return Requests.indexRequest()
//                        .index("my-index")
//                        .type("my-type")
//                        .source(json);
//            }
//
//            @Override
//            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
//                indexer.add(createIndexRequest(element));
//
//            }
//        }, new ActionRequestFailureHandler() {
//            @Override
//            public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
//                if(ExceptionUtils.findThrowable(failure,EsRejectedExecutionException.class).isPresent()) {
//                    indexer.add(action);
//
//                } else if (ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()) {
//                        // 添加自定义的处理逻辑
//                    } else {
//                        throw failure;
//                    }
//                }
//            }
//        ));
//        System.out.println("*********** hdfs ***********************");
////        BucketingSink<String> bucketingSink = new BucketingSink<>("hdfs://10.27.1.141:9000/user/bigdata/lihao/test/"); //hdfs上的路径
//        System.setProperty("HADOOP_USER_NAME","bigdata");
//        BucketingSink<String> bucketingSink = new BucketingSink<>("hdfs://10.3.6.7:9000/user/hadoop/"); //hdfs上的路径
////        BucketingSink<String> bucketingSink1 = bucketingSink.setBucketer((Bucketer<String>) (clock, basePath, value) -> {
////            return basePath;
////        });
////        bucketingSink.setFSConfig()
//        bucketingSink.setWriter(new StringWriter<String>())
////                .setBatchSize(1024 * 1024 * 64 )
//                .setUseTruncate(false)
//                .setBatchRolloverInterval(1000*60*60);
//        messageStream.addSink(bucketingSink);
        env.execute();
    }
}

