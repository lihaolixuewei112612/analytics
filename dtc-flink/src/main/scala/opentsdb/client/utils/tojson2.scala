package opentsdb.client.utils

import com.google.gson.Gson
import scala.collection.JavaConversions

import scala.collection.mutable


/**
  * Created on 2019-06-19
  *
  * @author :hao.li
  */
object tojson2 {
  def main(args: Array[String]): Unit = {
    val myMap = mutable.HashMap("foo" -> "baz")
    val gson = new Gson()
    var bac =gson.toJson(toJava(myMap))
    print(bac)
  }

  def toJava(x: Any): Any = {
    import scala.collection.JavaConverters._
    x match {
      case y: scala.collection.MapLike[_, _, _] =>
        y.map { case (d, v) => toJava(d) -> toJava(v) } asJava
      case y: scala.collection.SetLike[_,_] =>
        y map { item: Any => toJava(item) } asJava
      case y: Iterable[_] =>
        y.map { item: Any => toJava(item) } asJava
      case y: Iterator[_] =>
        toJava(y.toIterable)
      case _ =>
        x
    }
  }

}
