package com.dtc.analytics.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
/**
 * @Author :lihao
 * @Date :Created in 14:50 2019-04-28
 */
public class Constants {
  public final static String UNKNOWN = "Unknown";
  public final static String ENCODING = "UTF-8";
  public final static String SPLIT_CHARACTERS_1 = "\001";
  public final static String SPLIT_CHARACTERS_2 = "\002";
  public final static String ACT_SPLIT_CHARACTER = "||";
  public final static String USER_ACTION_CHARCTER = "|";
  public final static String DATE_APPKEY_USERID = "==";
  public final static int HBASE_SCANNER_CACHING = 20000;
  public final static int ACCESS_LEVEL_LIMIT = 6;
  public final static Date INITIAL_DATE;
  public final static String ACT_EXIT = "Exit";
  public final static int TOP_N_APP = 100;
  public final static int[] RANGES = {7, 14, 30};
  private final static Logger LOGGER = LoggerFactory.getLogger(Constants.class);

  static { // Set INITIAL_DATE.
    String str = "2014-01-01";
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    Date date = new Date();

    try {
      date = sdf.parse(str);
    } catch (ParseException e) {
      LOGGER.error("An error was caught", e);
    }
    INITIAL_DATE = (Date) date.clone();
  }

}
