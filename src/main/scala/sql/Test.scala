package sql

import java.util.Properties

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession


/**
  * 行转列和列转行
  * @author hongtao.hao
  * @date 2020/6/3
  */
object Test {
  def dbConnProperties(): Properties = {
    val ConnProperties = new Properties();
    ConnProperties.put("driver", "com.mysql.jdbc.Driver");
    ConnProperties.put("user", "root");
    ConnProperties.put("password", "root");
    ConnProperties.put("fetchsize", "1000"); //读取条数限制
    ConnProperties.put("batchsize", "10000"); //写入条数限制
    return ConnProperties;
  }

  def main(args: Array[String]): Unit = {

    val sSession = new SparkSession
    .Builder()
      .appName("Test")
      .master("local")
      .getOrCreate()


    val url = "jdbc:mysql://localhost:3306/data?useUnicode=true&characterEncoding=utf8"

    val df = sSession.read.jdbc(url,"tb_score",dbConnProperties())

    df.createTempView("tb_score")

    val score = sSession.sql("select * from tb_score")

    score.show()

    val res = sSession.sql("SELECT userid,\nSUM(CASE `subject` WHEN '语文' THEN score ELSE 0 END) as '语文',\nSUM(CASE `subject` WHEN '数学' THEN score ELSE 0 END) as '数学',\nSUM(CASE `subject` WHEN '英语' THEN score ELSE 0 END) as '英语',\nSUM(CASE `subject` WHEN '政治' THEN score ELSE 0 END) as '政治' \nFROM tb_score \nGROUP BY userid")

    res.show()
  }

}
