package com.dtc.analytics.works;

import com.dtc.analytics.common.DtcConf;
import com.dtc.analytics.common.Formatter;
//import com.dtc.analytics.common.HBaseUtils;
import com.dtc.analytics.common.HBaseUtils;
import com.dtc.analytics.mrthread.HourlyHdfs2EsThread;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Hourly {
    private final static String HBASE_NAME = "mr_result";

    private static String workID = null;
    private static boolean isSucc = false;
    private static Logger logger = LoggerFactory.getLogger(Hourly.class);

    public static void main(String[] args) {
        DtcConf.setup();
        try {
            String prevHour = Formatter.getPrevHour(); // return: yyyyMMddHH
            workID = "Hourly-" + prevHour;
            isSucc = hourlyWork(prevHour, workID);
        } catch (IOException e) {
            logger.warn("Hourly worker is failed by in {}.", e);
        } finally {
            HBaseUtils.insterRow(HBASE_NAME, workID, "f", "event", String.valueOf(isSucc));
        }
    }

        private static boolean hourlyWork (String dateHour, String workID) throws IOException {
            boolean isSucc = false;
            Configuration conf = DtcConf.getConf();
            FileSystem fs = null;

            fs = FileSystem.get(conf);
            HourlyHdfs2EsThread hourlyHdfs2EsThread = new HourlyHdfs2EsThread(dateHour, workID, fs);

            try {
                hourlyHdfs2EsThread.start();
                hourlyHdfs2EsThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            fs.close();
            isSucc = hourlyHdfs2EsThread.isSucc();
            return isSucc;
        }
    }
