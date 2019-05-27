package com.dtc.analytics.works;

import com.dtc.analytics.common.DtcConf;
import com.dtc.analytics.common.Formatter;
import com.dtc.analytics.mrthread.DailyHdfs2Es;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 2019-04-30
 *
 * @author :hao.li
 */
public class Daily {
    private static Logger logger = LoggerFactory.getLogger(Daily.class);

    public static void main(String[] args) {
        DtcConf.setup();
//        String dateHour = getDate();
        String prevDay = Formatter.getPrevDay(); // return: yyyyMMdd
        String workID = "Daily-" + prevDay;
        boolean isSucc = false;
        try {
            isSucc = dailyWork(prevDay, workID);
        } catch (Exception e) {
            logger.warn("Daily worker is failed by in {}.", e);
        }
    }

    private static boolean dailyWork(final String daily, final String workID) throws Exception {
        boolean isSucc;
        // Initial
        Configuration conf = DtcConf.getConf();
        FileSystem fs = FileSystem.get(conf);
        DailyHdfs2Es eventThread = new DailyHdfs2Es(daily, workID, fs);
        // Start
        eventThread.start();
        fs.close();
        // If succeed
        isSucc = eventThread.isSucc();
        return isSucc;
    }
}
