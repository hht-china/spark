package sql

import org.apache.spark
import org.apache.spark.rdd.RDD
import rdd.Order

/**
  * @author hongtao.hao
  * @date 2020/6/5
  */
object OrderTime {
  def main(args: Array[String]): Unit = {
    val sparkSession = new spark.sql.SparkSession
    .Builder()
      .appName("AccumulatorTest")
      .master("local")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    val rdd: RDD[String] = sc.parallelize(List(
      "1;1;2017-07-21 09:43:02",
      "2;12;2017-07-25 11:37:48",
      "3;10;2017-07-25 11:43:41",
      "4;1;2017-07-27 01:27:22",
      "5;10;2017-07-27 07:46:45",
      "6;1;2017-07-27 10:21:37",
      "7;12;2017-07-27 13:26:19"
    ))

    rdd.foreach(println)

    val flat_rdd = rdd.map(x => {
       val sarr = x.split(";")
      (sarr(0),sarr(1),sarr(3))
    })
    flat_rdd.foreach(println)

//    val order_rdd = flat_rdd.map(record => new Order(record(0).toInt,record(1).toInt,record(2).toString))
//    order_rdd.foreach(println)
//
//    import sparkSession.implicits._
//
//    val order_df = order_rdd.toDF()
//    order_df.show()

  }

  case class Order(id:Int,phoneNumber:Int,time:String)

}
