package com.dtc.analytic.scala.demo

import com.dtc.analytic.scala.demo.EnumTest.WeekDay

/**
  * Created on 2019-05-30
  *
  * @author :hao.li
  */
object Main {
  object WeekDay extends Enumeration {
    type WeekDay = Value//这里仅仅是为了将Enumration.Value的类型暴露出来给外界使用而已
    val Mon, Tue, Wed, Thu, Fri, Sat, Sun = Value//在这里定义具体的枚举实例
  }
  import WeekDay._

  def isWorkingDay(d: WeekDay) = ! (d == Sat || d == Sun)

//  WeekDay.values filter isWorkingDay foreach println//使用语法糖进行输出
  def getIndex(name: String): Int = {
    for (c <- WeekDay.values) {
      if (c.toString==name){
        return c.id+1
      }
    }
    1
  }

  def main(args: Array[String]): Unit = {
    print(getIndex("Sun"))
  }
}
