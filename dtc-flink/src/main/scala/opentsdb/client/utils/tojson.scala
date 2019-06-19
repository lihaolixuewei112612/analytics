package opentsdb.client.utils

import com.google.gson.Gson

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created on 2019-06-19
  *
  * @author :hao.li
  */
object tojson {
  def main(args: Array[String]): Unit = {
    val gson = new Gson()
    val myMap = mutable.HashMap("foo" -> "baz")
    val serializedMap = gson.toJson(myMap.asJava)
    print(serializedMap)
    var user = User(1, 2, "lihao", gson.toJson(mutable.HashMap("1" -> "2", "kk" -> "jj").asJava), "test", false)
    var result = gson.toJson(user)
    print(result)

  }

}

case class User(id: Long, organization_id: Long, username: String, role_names: String, role_ids: String, locked: Boolean)

