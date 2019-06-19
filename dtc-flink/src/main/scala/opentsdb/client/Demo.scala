package opentsdb.client

import opentsdb.client.builder.MetricBuilder

/**
  * Created on 2019-06-19
  *
  * @author :hao.li
  */
object Demo {
  def main(args:Array[String]): Unit ={
    val client = new HttpClientImpl
    client.HttpClientImpl(("http://10.3.6.12:4242"))
    val builder = new MetricBuilder
    builder.addMetric("metric2").setDataPoint(200L).addTag("lihao", "tab1value").addTag("tag2", "tab2value")
    val response = client.pushMetrics(builder,ExpectResponse.SUMMARY)
    System.out.println(response)
  }

}
