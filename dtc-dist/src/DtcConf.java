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
    private final static String DTC_FILE = "dtc-mr.xml";


    /**
     * Initial or refresh configuration & constants. Before you starting
     * applications on Hadoop/HBase, you should call this method at first. If you
     * are testing units, calling this method might not be necessary.
     */
    public static void setup() {
        InputStream inputStream = null;
        try {
            inputStream = ClassLoader.getSystemResourceAsStream(DTC_FILE);
            //local test
//      inputStream = new FileInputStream(new File("/Users/lixuewei/workspace/private/dtcanalytics/conf/dtc-mr.xml"));
            if (null != inputStream) {
                conf.addResource(inputStream);
            } else {
                throw new IOException("Configure file not found: razor-mr.xml");
            }
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


}
