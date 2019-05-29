package com.dtc.analytic.scala.common

import java.io.{File, FileInputStream, FileNotFoundException, IOException, InputStream}
import java.nio.file.Path
import org.slf4j.{Logger, LoggerFactory}
import org.apache.hadoop.conf.Configuration

/**
  * Created on 2019-05-27
  *
  * @author :hao.li
  */
object DtcConf {

  def logger: Logger = LoggerFactory.getLogger(DtcConf.getClass)

  private val conf = new Configuration()

  def setup(): Unit = {
    var inputStream: InputStream = null
    try {
//      var inputStream = new FileInputStream(new File("conf/dtc-flink.xml"))
      var inputStream = DtcConf.getClass.getClassLoader.getResourceAsStream("dtc-flink.xml")
      if (inputStream != null) {
        conf.addResource(inputStream)
      } else {
        throw new IOException("Configure file not found: dtc-flink.xml")
      }
    } catch {
      case ex: FileNotFoundException => {
        logger.error("Configure file not found: dtc-flink.xml,and the cause is {}.", ex)
      }
      case ex: IOException => {
        logger.error("Configure file not found: dtc-flink.xml,and the cause is {}.", ex)
      }

    } finally {
      if (inputStream != null) {
        inputStream.close()
      }
    }

  }

  def getConf(): Configuration = {
    return conf
  }

}
