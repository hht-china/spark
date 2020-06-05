package rdd

import org.apache.spark
import org.apache.spark.rdd.RDD

/**
  * @author hongtao.hao
  * @date 2020/6/5
  */
object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = new spark.sql.SparkSession
    .Builder()
      .appName("AccumulatorTest")
      .master("local")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    val longAccumulator  = sc.longAccumulator("AccumulatorTest")

    val list_rdd: RDD[String] = sc.parallelize( List("rose is beautiful",  "jisoo is beautiful"))
    val map_rdd = list_rdd.map(x => {
      longAccumulator.add(1)
    })
    map_rdd.cache()
    map_rdd.collect()
    map_rdd.collect()
    println(longAccumulator.value)  //2


  }
}
