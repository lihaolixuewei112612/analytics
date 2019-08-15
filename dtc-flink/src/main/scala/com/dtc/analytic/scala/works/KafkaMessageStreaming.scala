package com.dtc.analytic.scala.works


import com.dtc.analytic.scala.common.DtcConf
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Created on 2019-06-06
  *
  * @author :hao.li
  */
object KafkaMessageStreaming {
  def main(args: Array[String]): Unit = {
    DtcConf.setup()
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    env.setParallelism(1)
    //    val conf = DtcConf.getConf()
    //    val level = conf.get("log.level")
    //    val lev: Int = LevelEnum.getIndex(level)
    //
    //    val brokerList = conf.get("flink.kafka.broker.list")
    //    val topic = conf.get("flink.kafka.topic")
    //    val groupId = conf.get("flink.kafka.groupid")
    //    val prop = new Properties()
    //    prop.setProperty("bootstrap.servers", brokerList)
    //    prop.setProperty("group.id", groupId)
    //    prop.setProperty("topic", topic)
    val flieStream = env.readTextFile("/Users/lixuewei/workspace/DTC/dtc-analytics_0.3/dtc-flink/src/main/resources/long_text_2019-07-22-11-57-56.txt")

    //    val myConsumer = new FlinkKafkaConsumer09[String](topic, new SimpleStringSchema(), prop)
    //    val waterMarkStream = myConsumer.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String)] {
    //      var currentMaxTimestamp = 0L
    //      var maxOutOfOrderness = 10000L // 最大允许的乱序时间是10s
    //      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //
    //      override def extractTimestamp(element: (String), previousElementTimestamp: Long) = {
    //        val event = element.split("\\$\\$")
    //        var time1 = event(0).replace("T", " ").split("\\+")(0).replace("-", "/")
    //        var time2 = new Date(time1).getTime
    //        currentMaxTimestamp = Math.max(time2, currentMaxTimestamp)
    //        time2
    //      }
    //
    //      override def getCurrentWatermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    //
    //    })
    //    val text = env.addSource(flieStream)
    //    var inputMap = flieStream.flatMap(new myFlatMapFunction).keyBy("System_name").keyBy("Host_ip").sum("value")
    var inputMap = flieStream.flatMap(new myFlatMapFunction).keyBy("System_name").keyBy("Host_ip").reduce(new ReduceFunction[DataStruct] {
      override def reduce(t: DataStruct, t1: DataStruct): DataStruct = {
        var test = t.value + t1.value
        DataStruct(t.System_name, t.Host_ip, "cup", t.time, test)
      }
    })
    inputMap.print()
    //    val result = inputMap.filter(b => b._1.equals("linux")).filter(a => a._3.equals("a") || a._3.equals("b")).keyBy(1).sum(4)
    //    var result = inputMap.keyBy(2).sum(4)
    //    result.print()

    //    var window = inputMap.keyBy(0)
    //      .timeWindow(Time.seconds(10), Time.seconds(5))
    env.execute("StreamingWindowWatermarkScala")
  }

  class myFlatMapFunction extends FlatMapFunction[String, DataStruct] {
    override def flatMap(t: String, collector: Collector[DataStruct]): Unit = {
      var message: DataStruct = null
      val jsonParser = new JSONParser()
      val json = jsonParser.parse(t).asInstanceOf[JSONObject]
      val name = json.get("name").toString.split("_", 2)
      val system_name = name(0).trim
      val zhibiao_name = name(1).trim
      if (system_name.contains("node")) {
        val time = json.get("timestamp").toString
        val value = json.get("value").toString.toDouble
        val lable = jsonParser.parse(json.get("labels").toString).asInstanceOf[JSONObject]
        val lable_ip = lable.get("instance").toString.trim
        if ("a".equals(zhibiao_name)) {
          message = DataStruct(system_name, lable_ip, zhibiao_name, time, value)
          collector.collect(message)
        } else if ("b".equals(zhibiao_name)) {
          message = DataStruct(system_name, lable_ip, zhibiao_name, time, value)
          collector.collect(message)
        } else if ("c".equals(zhibiao_name)) {
          message = DataStruct(system_name, lable_ip, zhibiao_name, time, value)
          collector.collect(message)
        } else if ("d".equals(zhibiao_name)) {
          message = DataStruct(system_name, lable_ip, zhibiao_name, time, value)
          collector.collect(message)
        } else {
          message = DataStruct(system_name, lable_ip, zhibiao_name, time, value * 0)
          collector.collect(message)
        }
      }

    }
  }


}

class MyMapFunction extends MapFunction[String, DataStruct] {
  override def map(line: String): DataStruct = {
    var message: DataStruct = null
    val jsonParser = new JSONParser()
    val json = jsonParser.parse(line).asInstanceOf[JSONObject]
    val name = json.get("name").toString.split("_", 2)
    val system_name = name(0).trim
    val zhibiao_name = name(1).trim
    if (system_name.contains("linux")) {
      val time = json.get("timestamp").toString
      val value = json.get("value").toString.toDouble
      val lable = jsonParser.parse(json.get("labels").toString).asInstanceOf[JSONObject]
      val lable_ip = lable.get("instance").toString.trim
      if ("a".equals(zhibiao_name)) {
        DataStruct(system_name, lable_ip, zhibiao_name, time, value)
        //        message.system_name=system_name
        //        message.lable_ip=lable_ip
        //        message.zhibiao_name=zhibiao_name
        //        message.time=time
        //        message.value=value
      }
    }
    //    val time = json.get("timestamp").toString
    //    val value = json.get("value").toString.toDouble
    //    val lable = jsonParser.parse(json.get("labels").toString).asInstanceOf[JSONObject]
    //    val lable_ip = lable.get("instance").toString
    //    val result = (name, lable_ip, time, value)
    return message
  }
}


//class OneMapFunction extends MapFunction[String,JSONObject]{
//  override def map(t: String): JSONObject = {
//    val jsonParser = new JSONParser()
//    val json = jsonParser.parse(t).asInstanceOf[JSONObject]
//    return json
//  }
//}
//
//case class DataStruct() {
//  var system_name: String = null
//  var lable_ip:String=null
//  var zhibiao_name:String= null
//  var time:String=null
//  var value = 0.0
//
////  def this(system_name: String,lable_ip:String,zhibiao_name:String,time:String, value: Double) {
////    this()
////    this.system_name = system_name
////    this.lable_ip = lable_ip
////    this.zhibiao_name=zhibiao_name
////    this.time = time
////    this.value=value
////  }
////
////  override def toString: String = "DataStruct{" + "system_name='" + system_name + '\'' + ", lable_ip=" + lable_ip + '}'
//}
