package com.dtc.analytics.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexFilter extends Configured implements PathFilter {
  private final static Logger logger = LoggerFactory.getLogger(RegexFilter.class);

  Pattern pattern;
  Configuration conf;
  FileSystem fs;

  public boolean accept(Path path) {
    try {
      if (fs.isDirectory(path)) {
        return true;
      } else {
        Matcher m = pattern.matcher(path.toString());
        return m.matches();
      }
    } catch (IOException e) {
      logger.error("An error was caught", e);
      return false;
    }

  }

  @Override public void setConf(Configuration conf) {
    this.conf = conf;
    if (conf != null) {
      try {
        fs = FileSystem.get(conf);
        pattern = Pattern.compile(conf.get("hdfs.file.pattern"));
      } catch (IOException e) {
        logger.error("An error was caught", e);
      }
    }
  }

}
