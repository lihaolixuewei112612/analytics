package com.dtc.analytics.common;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Formatter {
    private final static Logger LOGGER = LoggerFactory.getLogger(Formatter.class);

    /**
     * Convert a hex string to byte array.
     *
     * @param s The string to be converted.
     * @return The byte array converted from s.
     */
    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] =
                    (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }


    /**
     * 根据用户传入的时间表示格式，返回yyyyMMddHHmmss格式的时间
     *
     * @param dateTime yyyy-MM-dd HH:mm:ss
     * @return
     */
    public static String trimDateTime(String dateTime) {
        if (StringUtils.isEmpty(dateTime)) {
            return null;
        }
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = df.parse(dateTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        return formatter.format(date);
    }


    /**
     * Generate row key with md5 prefix for HTable.
     *
     * @param body
     * @return Result in format: md5(<code>body</code>).substring(0,
     * <code>prefixLen</code>)-body
     */
    public static String genMD5RowKey(String conn, String body, int prefixLen) {
        String prefix = MD5.md5Hashing(body).substring(0, prefixLen);
        return stringConnector(conn, prefix, body);
    }

    /**
     * Change byte[] to date string with short format (yyyyMMdd)
     *
     * @param byteDate byte[] date with format: "yyyy-MM-dd HH:mm:ss".
     * @return Day of date: yyyyMMdd.
     */
    static public String bytes2DateDay(byte[] byteDate) {
        if (byteDate == null)
            return null;
        try {

            String strDate = new String(byteDate);
            SimpleDateFormat formatshort = new SimpleDateFormat("yyyyMMdd");
            SimpleDateFormat formatlong = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = null;

            date = formatlong.parse(strDate);

            return formatshort.format(date);
        } catch (ParseException e) {
            return null;
        }
    }


    /**
     * Connect multiple strings with specific string connector.
     *
     * @param conn    String connector.
     * @param collect Strings separated by comma or String[].
     * @return The combined result.
     */
    static public String stringConnector(String conn, String... collect) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < collect.length; i++) {
            sb.append(collect[i]);
            if (i < collect.length - 1) {
                sb.append(conn);
            }
        }
        return sb.toString();
    }

    /**
     * Get tomorrow string according to {@code currentDay}.
     *
     * @param currentDay Current day string: yyyyMMdd.
     * @return Tomorrow string: yyyyMMdd.
     * @throws ParseException
     */
    public static String getNextDay(String currentDay) throws ParseException {
        Date tdate = null;
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        try {
            tdate = format.parse(currentDay);

            cal.setTime(tdate);
            cal.add(Calendar.DATE, 1);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String date = format.format(cal.getTime());
        return date;
    }

    /**
     * Get one hour before current time for later start time. Output format:
     * yyyyMMddHH
     *
     * @param input
     * @return
     */
    public static String getPrevHour(String input) {

        Calendar cal = Calendar.getInstance();
        Date date = null;
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHH");
        try {
            date = format.parse(input);
        } catch (ParseException e) {
            LOGGER.error("ParseException was caught", e);
        }

        cal.setTime(date);

        cal.add(Calendar.HOUR_OF_DAY, -1);
        String hour = format.format(cal.getTime());
        return hour;

    }

    /**
     * get the last hour
     */
    public static String getPrevHour() {
        Date data = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHH");
        String formatDate = format.format(data);
        String prevHour = getPrevHour(formatDate);
        return prevHour;
    }

    /**
     * Get days counting from initial date.
     *
     * @param date
     * @return
     * @throws ParseException
     */
    static public int getDays(Date date) throws ParseException {
        Date zerodate = Constants.INITIAL_DATE;
        return (int) ((date.getTime() - zerodate.getTime()) / (3600 * 24 * 1000));
    }

    /**
     * Get hours counting from initial date.
     *
     * @param date
     * @return
     * @throws ParseException
     */
    static public int getHours(Date date) throws ParseException {
        Date zerodate = Constants.INITIAL_DATE;
        return (int) ((date.getTime() - zerodate.getTime()) / (3600 * 1000));
    }

    /**
     * Get previous day in format: yyyyMMdd.
     *
     * @param curDayStr
     * @return
     */
    public static String getPrevDay(String curDayStr) {
        String prevDayStr = null;
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        Date date = null;
        try {
            date = format.parse(curDayStr);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.DATE, -1);
            prevDayStr = format.format(cal.getTime());
        } catch (ParseException e) {
            LOGGER.error("An error was caught", e);
        }
        return prevDayStr;
    }
    public static String getPrevDay(){
        Date data = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        String format1 = format.format(data);
        String prevHour = getPrevDay(format1);
        return prevHour;

    }

    /**
     * Generate prefix of row key for HTables: *_Product_Device.
     *
     * @param key
     * @param length
     * @return
     */
    public static String genPrefix(String key, int length) {
        String rowKey = null;
        String md5Key;
        try {
            md5Key = MD5.md5Hashing(key);
            rowKey = md5Key.substring(0, length - 1) + "_" + key;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rowKey;
    }

    /**
     * Get the last day in the month which date specified.
     *
     * @param date
     * @return The last day in the month which date specified.
     */
    public static int getMonthDays(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        return c.getActualMaximum(Calendar.DAY_OF_MONTH);
    }

    /**
     * Get previous month in specific format.
     *
     * @param dateDay Date format: yyyyMMdd
     * @param fmtOut  Date format of result.
     * @return Previous month in format of <code>fmtOut</code>.
     * @throws ParseException
     */
    public static String getPrevMonth(String dateDay, String fmtOut) throws ParseException {
        final SimpleDateFormat sdfIn = new SimpleDateFormat("yyyyMMdd");
        final SimpleDateFormat sdfOut = new SimpleDateFormat(fmtOut);
        Calendar cal = Calendar.getInstance();
        cal.setTime(sdfIn.parse(dateDay));
        cal.add(Calendar.MONTH, -1);
        String monthStr = sdfOut.format(cal.getTime());
        return monthStr;
    }

    /**
     * Compares two version strings.
     * <p/>
     * Use this instead of String.compareTo() for a non-lexicographical comparison
     * that works for version strings. e.g. "1.10".compareTo("1.6").
     *
     * @param version1 a string of ordinal numbers separated by decimal points.
     * @param version2 a string of ordinal numbers separated by decimal points.
     * @return The result is a negative integer if version1 is _numerically_ less
     * than version2. The result is a positive integer if version1 is
     * _numerically_ greater than version2. The result is zero if the
     * strings are _numerically_ equal.
     * @note It does not work if "1.10" is supposed to be equal to "1.10.0".
     */
    public static Integer versionCompare(String version1, String version2) {
        version1 = version1.replaceAll("[^0-9.]", "");
        version2 = version2.replaceAll("[^0-9.]", "");
        if (version1.isEmpty() || version2.isEmpty()) {
            // If any of the two version string is empty,
            // the version string which has longer length will be the later one.
            return Integer.signum(version1.length() - version2.length());
        }
        String[] vals1 = version1.split("\\.");
        String[] vals2 = version2.split("\\.");
        int i = 0;
        // set index to first non-equal ordinal or length of shortest version string
        while (i < vals1.length && i < vals2.length && vals1[i].equals(vals2[i])) {
            i++;
        }
        // compare first non-equal ordinal number
        if (i < vals1.length && i < vals2.length) {
            int diff = Integer.valueOf(vals1[i]).compareTo(Integer.valueOf(vals2[i]));
            return Integer.signum(diff);
        }
        // the strings are equal or one string is a substring of the other
        // e.g. "1.2.3" = "1.2.3" or "1.2.3" < "1.2.3.4"
        else {
            return Integer.signum(vals1.length - vals2.length);
        }
    }

    public static String genHiveRecord(String[] strs, JSONObject jsonObj) {
        StringBuilder sb = new StringBuilder();

        for (String str : strs) {
            sb.append(jsonObj.optString(str).replace("\n", "").replace("\r", "").replace("\001", ""));
            sb.append("\001");
        }
        sb.delete(sb.lastIndexOf("\001"), sb.length());
        return sb.toString();
    }

    public static String[] clearSeperatorChar(String[] params, String oldSeperator, String newSeperator) {
        String[] newParams = new String[params.length];
        for (int i = 0; i < params.length; i++) {
            newParams[i] = params[i].replaceAll(oldSeperator, newSeperator);
        }
        return newParams;
    }

}
