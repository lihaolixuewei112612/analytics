package com.dtc.analytics.mrthread;

import com.dtc.analytics.mapreduce.HourHdfs2EsMR;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 2019-04-30
 *
 * @author :hao.li
 */
public class DailyHdfs2Es extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(DailyHdfs2Es.class);

    private String daily;
    private String workID;
    private FileSystem fs;
    private boolean isSucc = false;

    public DailyHdfs2Es(String daily, String workID, FileSystem fs) {
        this.daily = daily;
        this.workID = workID;
        this.fs = fs;
    }

    public boolean isSucc() {
        return isSucc;
    }

    @Override
    public void run() {
        try {

            HourHdfs2EsMR eventStat = new HourHdfs2EsMR(daily, workID);
            eventStat.start();
            eventStat.join();

            if (!eventStat.isSucc()) {
                LOGGER.error("HourHdfs2EsMR failed");
            }
            isSucc = eventStat.isSucc();
        } catch (Exception e) {
            LOGGER.error("Exception was caught", e);
        }
    }
}
