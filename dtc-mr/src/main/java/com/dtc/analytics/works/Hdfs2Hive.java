package com.dtc.analytics.works;

import com.dtc.analytics.common.DtcConf;
import com.dtc.analytics.common.Formatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 2019-04-30
 *
 * @author :hao.li
 */
public class Hdfs2Hive {
    private static Logger logger = LoggerFactory.getLogger(Hdfs2Hive.class);

    public static void main(String[] args) {
        DtcConf.setup();
        String prevHour = Formatter.getPrevHour(); // return: yyyyMMddHH
        String workID = "Hdfs2Hive-" + prevHour;
        boolean isSucc = false;
        try {
//            isSucc = hourlyWork(prevHour, workID);
        } catch (Exception e) {
            logger.warn("Hourly worker is failed by in {}.", e);
        }
    }

//    private static boolean hourlyWork(final String dateHour, final String workID) throws Exception {
//        boolean isSucc;
//        // Initial
//        Configuration conf = DtcConf.getConf();
//        FileSystem fs = FileSystem.get(conf);
//        HourlyHdfs2EsThread eventThread = new HourlyHdfs2EsThread(dateHour, workID, fs);
//        // Start
//        eventThread.start();
//        fs.close();
//        // If succeed
//        isSucc = eventThread.isSucc();
//        return isSucc;
//    }
}
