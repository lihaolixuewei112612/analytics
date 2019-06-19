package opentsdb.client.builder



import java.io.IOException

import com.google.gson.{Gson, GsonBuilder}

import scala.collection.mutable
import com.google.common.base.Preconditions.checkState

/**
  * Created on 2019-06-18
  *
  * @author :hao.li
  */
class MetricBuilder {
  var metrics:mutable.MutableList[Metric]= mutable.MutableList()
  var mapper: Gson = new Gson()


  def MetricBuilder: Unit = {
    val builder = new GsonBuilder
    mapper = builder.create
  }

  def getInstance: MetricBuilder = {
    return new MetricBuilder
  }

  /**
    * Adds a metric to the builder.
    *
    * @param metricName
    * metric name
    * @return the new metric
    */
  def addMetric(metricName: String): Metric = {
    var metric = new Metric(metricName)
    metrics+=metric
    metric
  }

  /**
    * Returns a list of metrics added to the builder.
    *
    * @return list of metrics
    */
  def getMetrics: mutable.MutableList[Metric] = metrics

  /**
    * Returns the JSON string built by the builder. This is the JSON that can
    * be used by the client add metrics.
    *
    * @return JSON
    * @throws IOException
    * if metrics cannot be converted to JSON
    */
  @throws[IOException]
  def build: String = {
    for (metric:Metric <- metrics) { // verify that there is at least one tag for each metric
      var t = metric.getTags
      var len = t.size
      var name = metric.getName
//      checkState(len > 0, metric.getName + " must contain at least one tag.")
    }
    var abc = mapper.toJson(metrics)
    print(abc)
    abc
  }

}

//object MetricBuilder{
//  def getInstance:MetricBuilder={
//    return new MetricBuilder
//  }
//
//}
