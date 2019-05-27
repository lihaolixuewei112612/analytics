package com.dtc.analytics.common;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;


/**
 * @Author :lihao
 * @Date :Created in 14:50 2019-04-28
 */
public class DtcConf {
  private static Logger logger = LoggerFactory.getLogger(DtcConf.class);
  private static Configuration conf = new Configuration();
  private static int reduceTaskNum = 9;
  private static String dbPrefix;



  /**
   * Initial or refresh configuration & constants. Before you starting
   * applications on Hadoop/HBase, you should call this method at first. If you
   * are testing units, calling this method might not be necessary.
   */
  public static void setup() {
    InputStream inputStream = null;
    try {
      inputStream = ClassLoader.getSystemResourceAsStream("dtc-mr.xml");
      //local test
//      inputStream = new FileInputStream(new File("/Users/lixuewei/workspace/private/dtcanalytics/conf/dtc-mr.xml"));
      if (null != inputStream) {
//        conf.set("hadoop.tmp.dir", "/tmp");
//        conf.set("fs.defaultFS", "hdfs://10.3.6.7:9000");
        conf.addResource(inputStream);
      } else {
        throw new IOException("Configure file not found: razor-mr.xml");
      }
      reduceTaskNum = conf.getInt("hbase.reduce.tasknum", reduceTaskNum);
      //      hBaseConn = ConnectionFactory.createConnection(conf);
      //       Sync HTables from SQL DB
      dbPrefix = conf.get("sql.razor.db.prefix", "razor_");
    } catch (FileNotFoundException e) {
      logger.error("An error was caught", e);
    } catch (IOException e) {
      logger.error("An error was caught", e);
    } catch (Exception e) {
      logger.error("An error was caught", e);
    } finally {
      try {
        if (inputStream != null) {
          inputStream.close();
        }
      } catch (Exception e) {
        logger.error("An error was caught", e);
      }
    }
  }


  public static Configuration getConf() {
    return conf;
  }


  public static int getReduceTaskNum() {
    return reduceTaskNum;
  }



}
