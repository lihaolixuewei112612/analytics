package com.dtc.analytic.scala.demo

import com.dtc.analytic.scala.demo.EnumTest.WeekDay

/**
  * Created on 2019-05-30
  *
  * @author :hao.li
  */
object Main {

  def main(args: Array[String]): Unit = {
   var str:String= "abc ###cbd"
   val sp =str.split("###")
    sp.foreach(e=>print(e.trim))
  }
}
