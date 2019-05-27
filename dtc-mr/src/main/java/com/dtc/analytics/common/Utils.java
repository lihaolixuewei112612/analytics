package com.dtc.analytics.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;


public class Utils {
  private final static Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  /**
   * To judge if there are any of fields null or empty.
   *
   * @param fields
   * @return If any of fields is null or empty, return false. Otherwise return
   * false.
   */
  public static boolean noAnyNullOrEmpty(byte[]... fields) {
    for (byte[] field : fields) {
      if (null == field || 0 == field.length) {
        return false;
      }
    }
    return true;
  }

  /**
   * To judge if there are any of fields null or empty.
   *
   * @param fields
   * @return If any of fields is null or empty, return false. Otherwise return
   * false.
   */
  public static boolean noAnyNullOrEmpty(String... fields) {
    for (String field : fields) {
      if (null == field || field.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Sync time to rowKey's time in hours.
   *
   * @param time Time to be adjusted.
   * @param rowKey Row key of original data contains time in hours:
   *        md5-yyyyMMddHH-uuid.
   * @return
   */
  // public static byte[] adjustTime(byte[] time, byte[] rowKey) {
  // String timeStr = Bytes.toString(time).trim();
  // int timeStrLen = timeStr.length();
  // String dateHour = Bytes.toString(rowKey).split("-")[1];
  // String year = dateHour.substring(0, 4);
  // String month = dateHour.substring(4, 6);
  // String day = dateHour.substring(6, 8);
  // String hour = dateHour.substring(8, 10);
  // timeStr =
  // String.format("%s-%s-%s %s:%s", year, month, day, hour,
  // timeStr.substring(timeStrLen - 5, timeStrLen));
  // return Bytes.toBytes(timeStr);
  // }

  /**
   * Sync time to rowKey's time in hours.
   *
   * @param time Time to be adjusted.
   * @param rowKey Row key of original data contains time in hours:
   *        md5-yyyyMMddHH-uuid.
   * @return
   */
  // public static String adjustTime(String time, String rowKey) {
  // String timeStr = time.trim();
  // int timeStrLen = timeStr.length();
  // String dateHour = rowKey.split("-")[1];
  // String year = dateHour.substring(0, 4);
  // String month = dateHour.substring(4, 6);
  // String day = dateHour.substring(6, 8);
  // String hour = dateHour.substring(8, 10);
  // timeStr =
  // String.format("%s-%s-%s %s:%s", year, month, day, hour,
  // timeStr.substring(timeStrLen - 5, timeStrLen));
  // return timeStr;
  // }



//  /**
//   * @param conf
//   * @return Duration segments in HTable "dimdurationsegments".
//   * @throws IOException
//   */
//  public static ArrayList<Segment> getDurSegments(Configuration conf) throws IOException {
//    ArrayList<Segment> dList = new ArrayList<Segment>();
//    // Get duration segment values from HBase
//    HTable dimDurSegHTable = new HTable(conf, MetaTables.DimDurationSegment.TABLE_NAME);
//    Scan scan = new Scan();
//    scan.setCaching(50);
//    scan.setCacheBlocks(false);
//
//    ResultScanner scanner = dimDurSegHTable.getScanner(scan);
//    Integer i = 0;
//    for (Result result : scanner) {
//      byte[] start =
//        result.getValue(Bytes.toBytes(MetaTables.COLUMN_FAMILY), Bytes.toBytes("start"));
//      byte[] end = result.getValue(Bytes.toBytes(MetaTables.COLUMN_FAMILY), Bytes.toBytes("end"));
//      if (start != null && end != null) {
//        Segment ds = new Segment(Bytes.toString(result.getRow()), Long.valueOf(new String(start)),
//          Long.valueOf(new String(end)));
//        dList.add(ds);
//        i++;
//      }
//    }
//    dimDurSegHTable.close();
//    return dList;
//  }
//
//  /**
//   * @param conf
//   * @return Session segment in HTable "dimsessionsegment".
//   * @throws IOException
//   */
//  public static ArrayList<Segment> getSessSegments(Configuration conf) throws IOException {
//    ArrayList<Segment> dList = new ArrayList<Segment>();
//    // Get session segment values from HBase
//    HTable dimSessSegTable = new HTable(conf, MetaTables.DimSessionSegment.TABLE_NAME);
//
//    Scan scan = new Scan();
//    scan.setCaching(50);
//    scan.setCacheBlocks(false);
//    ResultScanner dimsscanner = dimSessSegTable.getScanner(scan);
//    Integer j = 0;
//    for (Result result : dimsscanner) {
//      byte[] start =
//        result.getValue(Bytes.toBytes(MetaTables.COLUMN_FAMILY), Bytes.toBytes("start"));
//      byte[] end = result.getValue(Bytes.toBytes(MetaTables.COLUMN_FAMILY), Bytes.toBytes("end"));
//      if (start != null && end != null) {
//        Segment ds = new Segment(Bytes.toString(result.getRow()), Long.valueOf(new String(start)),
//          Long.valueOf(new String(end)));
//        dList.add(ds);
//        j++;
//      }
//    }
//    dimSessSegTable.close();
//    return dList;
//  }

  /**
   * Get local time and adjust time according to dateHour.
   *
   * @param dateHour Date time string. Format: yyyyMMddHH
   * @return Date time: yyyy-MM-dd HH:mm:ss with yyyy, MM, dd, HH in dateHour
   * and mm, ss in local time.
   */
  public static String getLocalTime(String dateHour) {
    final String year = dateHour.substring(0, 4);
    final String month = dateHour.substring(4, 6);
    final String day = dateHour.substring(6, 8);
    final long millis = System.currentTimeMillis();
    String sec = String.valueOf((millis / 1000) % 60);
    if (Integer.valueOf(sec) < 10) {
      sec = "0" + sec;
    }
    String min = String.valueOf((millis / 1000 / 60) % 60);
    if (Integer.valueOf(min) < 10) {
      min = "0" + min;
    }
    String hour = String.valueOf((millis / 1000 / 60 / 60) % 24);
    if (Integer.valueOf(hour) < 10) {
      hour = "0" + hour;
    }
    // Connect date time string with format: yyyy-MM-dd HH:mm:ss
    return (year + "-" + month + "-" + day + " " + hour + ":" + min + ":" + sec);
  }

//  /**
//   * Sync versions in HBase adn SQL DB. Don't call it in Mapper or Reducer.
//   */
//  public static void insVerIntoDB() {
//    final SQLDatabase sqlDB;
//    try {
//      sqlDB = new SQLDatabase();
//    } catch (SQLException e) {
//      LOGGER.error("SQLException was caught", e);
//      return;
//    } catch (ClassNotFoundException e) {
//      LOGGER.error("ClassNotFoundException was caught", e);
//      return;
//    }
//    try {
//      Scan scan = new Scan();
//      scan.setCaching(
//        RazorConf.getConf().getInt("hbase.razor.scanner.caching", Constants.HBASE_SCANNER_CACHING));
//      ResultScanner rScanner =
//        RazorConf.getHBaseConn().getTable(TableName.valueOf(MetaTables.RazorVersion.TABLE_NAME))
//          .getScanner(scan);
//      for (Result result : rScanner) {
//        for (Cell cell : result.rawCells()) {
//          String productID = Bytes.toString(result.getRow());
//          String version = Bytes.toString(CellUtil.cloneQualifier(cell));
//          String sql1 = "select 1 from razor_version where product_id=? and version=?";
//          String sql2 = "insert into razor_version(product_id,version) values(?,?)";
//          LOGGER.info("Executing SQL: " + sql1);
//          ResultSet rs = sqlDB.execQuery(sql1, productID, version);
//          if (!rs.next()) {
//            LOGGER.info("Executing SQL: " + sql2);
//            sqlDB.execUpdate(sql2, productID, version);
//          }
//        }
//      }
//    } catch (IOException e) {
//      LOGGER.error("IOException was caught", e);
//    } catch (SQLException e) {
//      LOGGER.error("SQLException was caught", e);
//    } finally {
//      sqlDB.close();
//    }
//  }

  public static String concatSetValue(Set<String> values){
    LOGGER.info("values size=" + values.size());
    String result = null;
    if (null != values && values.size() > 0){

      StringBuilder sbCity = new StringBuilder();
      for (String city : values){
        sbCity.append(city).append(Constants.USER_ACTION_CHARCTER);
      }
      sbCity.delete(sbCity.lastIndexOf(Constants.USER_ACTION_CHARCTER),sbCity.length());
      result = sbCity.toString();
    }
    return result;

  }
}
