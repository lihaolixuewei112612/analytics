package opentsdb.client

import opentsdb.client.builder.MetricBuilder
import opentsdb.client.request.QueryBuilder
import opentsdb.client.response.{Response, SimpleHttpResponse}


/**
  * Created on 2019-06-17
  *
  * @author :hao.li
  */
trait Client {
  val PUT_POST_API = "/api/put"

  val QUERY_POST_API = "/api/query"

  /**
    * Sends metrics from the builder to the KairosDB server.
    *
    * @param builder
    * metrics builder
    * @return response from the server

    * problem occurred sending to the server
    */
  def pushMetrics(builder: MetricBuilder): Response

  def pushQueries(builder: QueryBuilder): SimpleHttpResponse

}
