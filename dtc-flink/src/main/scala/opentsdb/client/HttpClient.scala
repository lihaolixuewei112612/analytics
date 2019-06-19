package opentsdb.client

import java.io.IOException

import opentsdb.client.ExpectResponse.ExpectResponse
import opentsdb.client.builder.MetricBuilder
import opentsdb.client.request.QueryBuilder
import opentsdb.client.response.{Response, SimpleHttpResponse}


/**
  * Created on 2019-06-17
  *
  * @author :hao.li
  */
trait HttpClient extends Client {
  @throws[IOException]
  def pushMetrics(builder: MetricBuilder, exceptResponse: ExpectResponse): Response

  @throws[IOException]
  def pushQueries(builder: QueryBuilder, exceptResponse: ExpectResponse): SimpleHttpResponse

}
