package com.dtc.analytics.mrthread;


import com.dtc.analytics.mapreduce.HourHdfs2EsMR;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @Author :lihao
 * @Date :Created in 14:50 2019-04-28
 */

public class HourlyHdfs2EsThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(HourlyHdfs2EsThread.class);

    private String dateHour;
    private String workID;
    private FileSystem fs;
    private boolean isSucc = false;

    public HourlyHdfs2EsThread(String dateHour, String workID, FileSystem fs) {
        this.dateHour = dateHour;
        this.workID = workID;
        this.fs = fs;
    }

    public boolean isSucc() {
        return isSucc;
    }

    @Override
    public void run() {
        try {
            HourHdfs2EsMR eventStat = new HourHdfs2EsMR(dateHour, workID);
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
