package com.dtc.analytic.scala.common

/**
  * Created on 2019-05-29
  *
  * @author :hao.li
  */
object Utils {

  def isEmpty(str: String): Boolean = {
    if (str.length == 0 || str == null) {
      return true
    } else {
      return false
    }
  }

}
