package sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @author hongtao.hao
  * @date 2019/7/8
  */
object RddTest {
  def main(args: Array[String]): Unit = {

    val sc = new SparkSession
    .Builder()
      .appName("source_data_mysql001")
      .master("local")
      .getOrCreate()

    val rdd = sc.sparkContext.parallelize(List(List("a b c", "a b b"), List("e f g", "a f g"), List("h i j", "a a b")))




    //TODO 保存为文件
    rdd.saveAsTextFile("./rdd/2")

    //TODO 读取
    val rddRead = sc.read.textFile("./rdd/2")
    rddRead.show()

    //TODO 检查点
    sc.sparkContext.setCheckpointDir("./rdd/1")
    rddRead.cache()
    rddRead.checkpoint()

  }
}
