package com.dtc.analytics.mapreduce;

import com.dtc.analytics.Struct.DataStruct;
import com.dtc.analytics.common.DtcConf;
import com.dtc.analytics.filter.RegexFilter;
import com.dtc.analytics.util.ObjectMapperFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import java.util.Locale;

public class HourHdfs2EsMR extends Thread {
    public final static String JOB_NAME = "MR-Hour";
    private final static Logger logger = LoggerFactory.getLogger(HourHdfs2EsMR.class);
    private static final ObjectMapper objectMapper = ObjectMapperFactory.getDefaultMapper();
    private String dateDay;
    private String dateHour;
    private boolean isSucc = false;

    /**
     * @param dateDay Date in format: yyyyMMdd.
     * @param workID  workID which the job belong to.
     */
    public HourHdfs2EsMR(String dateDay, String workID) throws Exception {
        if (dateDay == null) {
            throw new NullPointerException("curDayStr is null");
        }
        if (!dateDay.matches("^\\d{10}$")) {
            throw new Exception("dateDay's format does not meet \"yyyyMMdd\"");
        }
        if (workID == null) {
            throw new NullPointerException("workID is null");
        }
        this.dateDay = dateDay.substring(0, 8);
        this.dateHour = dateDay.substring(8);
    }

    public boolean isSucc() {
        return isSucc;
    }

    @Override
    public void run() {
        logger.info("HourHdfs2EsMR starting ...");
        try {
            Configuration conf = DtcConf.getConf();
            conf.set("dateday", dateDay);
            conf.setBoolean("mapred.map.tasks.speculative.execution", false);
            conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
//            conf.set("hadoop.tmp.dir", "/tmp");
//            conf.set("fs.defaultFS", "hdfs://10.3.6.7:9000");
            conf.set(ConfigurationOptions.ES_NODES, conf.get("dtc.es.nodes", "localhost:9200"));
            //ElaticSearch Index/Type
            conf.set(ConfigurationOptions.ES_RESOURCE, conf.get("dtc.es.indexandtype"));
            //Hadoop上的数据格式为JSON,可
            // 以直接导入
            conf.set(ConfigurationOptions.ES_INPUT_JSON, "yes");

            final FileSystem fs = FileSystem.get(conf);
            final String inPathPrefix = conf.get("hdfs.event.path",
                    "/user/dtc/event/");
            Path inPath = new Path(inPathPrefix + dateDay + "/" + dateHour);
            if (!fs.exists(inPath)) {
                logger.error("Input path not found: {}.", inPath.toString());
                isSucc = true;
                return;
            }
            FileStatus[] fStatuses = fs.listStatus(inPath);
            if (fStatuses != null && fStatuses.length > 0) {
                DistributedCache.addFileToClassPath(new Path("/user/dtc/elasticsearch-hadoop-6.7.1.jar"), conf);
                DistributedCache.addFileToClassPath(new Path("/user/dtc/commons-httpclient-3.1.jar"), conf);
                Job job = Job.getInstance(conf, JOB_NAME);
                job.setJarByClass(HourHdfs2EsMR.class);

//                FileInputFormat.setInputPathFilter(job, RegexFilter.class);
                boolean hasDir = false;
                for (FileStatus fStatus : fStatuses) {
                    if (fStatus.isDirectory()) {
                        FileInputFormat.addInputPath(job, fStatus.getPath());
                        hasDir = true;
                    }
                }
                if (!hasDir) {
                    logger.warn("No any directories in path: {}.", inPath.toString());
                    isSucc = true;
                    return;
                }
                job.setMapperClass(EventMapper.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputFormatClass(EsOutputFormat.class);
                // Configure Mapper

                isSucc = job.waitForCompletion(true);
            } else {
                String msg = "There is no any subdirectories found in path:" + inPath.toString()
                        + "\nExiting HourHdfs2EsMR ...";
                logger.error(msg);
            }
        } catch (ClassNotFoundException e) {
            logger.error("File not find ,the cause is {}.", e);
        } catch (IOException e) {
            logger.error("Exception was caught,the cause is {}.", e);
        } catch (InterruptedException e) {
            logger.error("Exception was caught,the cause is {}.", e);
        } finally {
            logger.info("HourHdfs2EsMR end.");
        }
    }

    public static class EventMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String level = null;
        private int lev;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            level = conf.get("log.level", "info");
            lev = LevelMap.getIndex(level.toLowerCase(Locale.ENGLISH));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

        }

        @Override
        public void map(LongWritable row, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            DataStruct dataStruct = new DataStruct();
            if (lev >= 1 && lev < 2) {
                if (text.contains("info") || text.contains("debug") || text.contains("notice") || text.contains("warning")
                        || text.contains("warn") || text.contains("err") || text.contains("crit") || text.contains("alert")
                        || text.contains("emerg") || text.contains("panic")) {
                    String message = fetchMessage(text, dataStruct);
                    context.write(new Text(""), new Text(message));
                }
            }
            if (lev >= 2 && lev < 3) {
                if (text.contains("debug") || text.contains("notice") || text.contains("warning")
                        || text.contains("warn") || text.contains("err") || text.contains("crit") || text.contains("alert")
                        || text.contains("emerg") || text.contains("panic")) {
                    String message = fetchMessage(text, dataStruct);
                    context.write(new Text(""), new Text(message));
                }
            }
            if (lev >= 3 && lev < 4) {
                if (text.contains("notice") || text.contains("warning")
                        || text.contains("warn") || text.contains("err") || text.contains("crit") || text.contains("alert")
                        || text.contains("emerg") || text.contains("panic")) {
                    String message = fetchMessage(text, dataStruct);
                    context.write(new Text(""), new Text(message));
                }
            }
            if (lev >= 4 && lev < 5) {
                if (text.contains("warning")
                        || text.contains("warn") || text.contains("err") || text.contains("crit") || text.contains("alert")
                        || text.contains("emerg") || text.contains("panic")) {
                    String message = fetchMessage(text, dataStruct);
                    context.write(new Text(""), new Text(message));
                }

            }
            if (lev >= 5 && lev < 6) {
                if (text.contains("err") || text.contains("crit") || text.contains("alert")
                        || text.contains("emerg") || text.contains("panic")) {
                    String message = fetchMessage(text, dataStruct);
                    context.write(new Text(""), new Text(message));
                }
            }
            if (lev >= 6 && lev < 7) {
                if (text.contains("crit") || text.contains("alert")
                        || text.contains("emerg") || text.contains("panic")) {
                    String message = fetchMessage(text, dataStruct);
                    context.write(new Text(""), new Text(message));
                }
            }
            if (lev >= 7 && lev < 8) {
                if (text.contains("alert")
                        || text.contains("emerg") || text.contains("panic")) {
                    String message = fetchMessage(text, dataStruct);
                    context.write(new Text(""), new Text(message));
                }
            } else {
                if (text.contains("emerg") || text.contains("panic")) {
                    String message = fetchMessage(text, dataStruct);
                    context.write(new Text(""), new Text(message));
                }
            }
        }

        private String fetchMessage(String text, DataStruct dataStruct) {
            String message = "";
            if (text.contains("$DTC$")) {
                String[] textSplit = text.split("\\$DTC\\$");
                String[] event = textSplit[0].split("\\$\\$");
                message += "{" + "\"time\"" + ":" + "\"" + event[0] + "\"" + "," + "\"device\"" + ":" + "\"" + event[1]
                        + "\"" + "," + "\"" + "level" + "\"" + ":" + "\"" + event[2] + "\"" + "," + "\"" + "hostname"
                        + "\"" + ":" + "\"" + event[3] + "\"" + "," + "\"" + "message" + "\"" + ":" + "\"" + event[4] + "\"";
                String str = "";
                for (int i = 1; i < textSplit.length; i++) {
                    str = textSplit[i] + "\n";
                }
                message += "\"" + "cause" + "\"" + ":" + "\"" + str + "\"" + "}";
            } else {
                String[] event = text.split("\\$\\$");
                message += "{" + "\"time\"" + ":" + "\"" + event[0] + "\"" + "," + "\"device\"" + ":" + "\"" + event[1]
                        + "\"" + "," + "\"" + "level" + "\"" + ":" + "\"" + event[2] + "\"" + "," + "\"" + "hostname"
                        + "\"" + ":" + "\"" + event[3] + "\"" + "," + "\"" + "message" + "\"" + ":" + "\"" + event[4] + "\"" + "}";
            }
            return message;
        }
    }
}
