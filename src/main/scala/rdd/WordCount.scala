package rdd

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.SparkSession

/**
  * @author hongtao.hao
  * @date 2020/6/5
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkSession = new spark.sql.SparkSession
    .Builder()
      .appName("wc")
      .master("local")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext

    val list = List("rose is beautiful", "jennie is beautiful", "lisa is beautiful", "jisoo is beautiful")

    val list_rdd: RDD[String] = sc.parallelize(list)
    list_rdd.foreach(println)

    //把一句话拆分成单词
    val flat_rdd = list_rdd.flatMap(_.split(" "))
    flat_rdd.foreach(println)

    //将每个单词变成kv结构
    val map_rdd = flat_rdd.map(word => (word,1))
    map_rdd.foreach(println)

    //聚合
    val reduce_rdd = map_rdd.reduceByKey(_+_)
    reduce_rdd.foreach(println)
  }


}
