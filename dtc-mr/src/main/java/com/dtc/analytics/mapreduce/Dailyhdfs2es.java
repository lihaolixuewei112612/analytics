package com.dtc.analytics.mapreduce;

import com.dtc.analytics.Struct.DataStruct;
import com.dtc.analytics.common.DtcConf;
import com.dtc.analytics.filter.RegexFilter;
import com.dtc.analytics.util.ObjectMapperFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.httpclient.util.DateParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created on 2019-04-30
 *
 * @author :hao.li
 */
public class Dailyhdfs2es extends Thread {
    public final static String JOB_NAME = "MR-Daily";
    private final static Logger logger = LoggerFactory.getLogger(Dailyhdfs2es.class);
    private static final ObjectMapper objectMapper = ObjectMapperFactory.getDefaultMapper();
    private String dateDay;
    private boolean isSucc = false;

    /**
     * @param dateDay Date in format: yyyyMMdd.
     * @param workID  workID which the job belong to.
     * @throws DateParseException
     */
    public Dailyhdfs2es(String dateDay, String workID) throws Exception {
        if (dateDay == null) {
            throw new NullPointerException("curDayStr is null");
        }
        if (!dateDay.matches("^\\d{8}$")) {
            throw new Exception("dateDay's format does not meet \"yyyyMMdd\"");
        }
        if (workID == null) {
            throw new NullPointerException("workID is null");
        }
        this.dateDay = dateDay.substring(0, 8);
    }

    public boolean isSucc() {
        return isSucc;
    }

    @Override
    public void run() {
        logger.info("Dailyhdfs2es start ...");
        String msg = "";
        try {
            // Then begin ...
            Configuration conf = DtcConf.getConf();
            conf.set("dateday", dateDay);
            conf.setBoolean("mapred.map.tasks.speculative.execution", false);
            conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

            //ElasticSearch节点
            conf.set(ConfigurationOptions.ES_NODES, conf.get("dtc.es.nodes", "localhost:9200"));
            //ElaticSearch Index/Type
            conf.set(ConfigurationOptions.ES_RESOURCE, conf.get("dtc.es.indexandtype"));
            //Hadoop上的数据格式为JSON,可以直接导入
            conf.set(ConfigurationOptions.ES_INPUT_JSON, "yes");

            final FileSystem fs = FileSystem.get(conf);
            final String inputPathPrefix = conf.get("hdfs.event.path",
                    "/user/dtc/event/");
            Path inputPath = new Path(inputPathPrefix + dateDay + "/*/");
            if (!fs.exists(inputPath)) {
                logger.error("Input path not found: {}.", inputPath.toString());
                isSucc = true;
                return;
            }
            FileStatus[] fStatuses = fs.listStatus(inputPath);
            if (fStatuses != null && fStatuses.length > 0) {
                Job job = new Job(conf, JOB_NAME);
                job.setJarByClass(HourHdfs2EsMR.class);

                // Input from HDFS
                FileInputFormat.setInputPathFilter(job, RegexFilter.class);
                boolean hasDir = false;
                for (FileStatus fStatus : fStatuses) {
                    if (fStatus.isDirectory()) {
                        FileInputFormat.addInputPath(job, fStatus.getPath());
                        hasDir = true;
                    }
                }
                if (!hasDir) {
                    logger.warn("No any directories found under path: {}", inputPath.toString());
                    isSucc = true;
                    return;
                }
                job.setMapperClass(Dailyhdfs2es.EventMapper.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setMapOutputKeyClass(NullWritable.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputFormatClass(EsOutputFormat.class);
                // Configure Mapper

                isSucc = job.waitForCompletion(true);
            } else {
                msg = "There is no any subdirectories found in path:" + inputPath.toString()
                        + "\nExiting HourHdfs2EsMR ...";
                logger.error(msg);
            }
        } catch (IOException e) {
            msg = e.getMessage();
            logger.error("Exception was caught", e);
        } catch (InterruptedException e) {
            msg = e.getMessage();
            logger.error("Exception was caught", e);
        } catch (ClassNotFoundException e) {
            msg = e.getMessage();
            logger.error("Exception was caught", e);
        } catch (Exception e) {
            msg = e.getMessage();
            logger.error("Exception was caught", e);
        } finally {
            logger.info("HourHdfs2EsMR end.");
        }
    }

    public static class EventMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

        }

        @Override
        public void map(LongWritable row, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            DataStruct dataStruct = new DataStruct();
            if (text.contains("debug")) {
                String message = fetchMessage(text, dataStruct);
                context.write(new Text(""), new Text(message));
            }
            if (text.contains("info")) {
                String message = fetchMessage(text, dataStruct);
                context.write(new Text(""), new Text(message));
            }
            if (text.contains("notice")) {
                String message = fetchMessage(text, dataStruct);
                context.write(new Text(""), new Text(message));
            }
            if (text.contains("warning") || text.contains("warn")) {
                String message = fetchMessage(text, dataStruct);
                context.write(new Text(""), new Text(message));
            }
            if (text.contains("err")) {
                String message = fetchMessage(text, dataStruct);
                context.write(new Text(""), new Text(message));
            }
            if (text.contains("crit")) {
                String message = fetchMessage(text, dataStruct);
                context.write(new Text(""), new Text(message));
            }
            if (text.contains("alert")) {
                String message = fetchMessage(text, dataStruct);
                context.write(new Text(""), new Text(message));
            }
            if (text.contains("emerg") || text.contains("panic")) {
                String message = fetchMessage(text, dataStruct);
                context.write(new Text(""), new Text(message));
            }
        }

        private String fetchMessage(String text, DataStruct dataStruct) {
            String message = "";
            if (text.contains("$DTC$")) {
                String[] textSplit = text.split("$DTC$");
                String[] event = textSplit[0].split("$$");
                dataStruct.setTime(event[0]);
                dataStruct.setDevice(event[1]);
                dataStruct.setLevel(event[2]);
                dataStruct.setHostname(event[3]);
                dataStruct.setMessage(event[4]);
                String str = "";
                for (int i = 1; i < textSplit.length; i++) {
                    str = textSplit[i] + "\n";
                }
                dataStruct.setCause(str);
            } else {
                String[] event = text.split("$$");
                dataStruct.setTime(event[0]);
                dataStruct.setDevice(event[1]);
                dataStruct.setLevel(event[2]);
                dataStruct.setHostname(event[3]);
                dataStruct.setMessage(event[4]);
            }
            try {
                message = objectMapper.writeValueAsString(dataStruct);
            } catch (JsonProcessingException e) {
                logger.error("Failed to write event into json,the cause is:{}!", e.getMessage());
            }
            return message;
        }
    }
}
