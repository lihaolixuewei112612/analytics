package com.dtc.analytics.common;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5 {
  /**
   * Hash a bytes array with MD5.
   *
   * @param plainByte Bytes array.
   * @return
   */
  public static String md5Hashing(byte[] plainByte) {
    if (plainByte == null) {
      return null;
    }
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");

      md.update(plainByte);
      byte b[] = md.digest();
      int i;
      StringBuffer buf = new StringBuffer("");
      for (int offset = 0; offset < b.length; offset++) {
        i = b[offset];
        if (i < 0)
          i += 256;
        if (i < 16)
          buf.append("0");
        buf.append(Integer.toHexString(i));
      }
      return buf.toString();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Hash <code>str</code> with MD5.
   *
   * @param str
   * @return MD5 summary string of <code>str</code>.
   */
  public static String md5Hashing(String str) {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("MD5");
      md.update(str.getBytes());
      byte byteData[] = md.digest();
      // convert the byte to hex format method 1
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < byteData.length; i++) {
        sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    return "";
  }

}
