package sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author hongtao.hao
  * @date 2019/7/2
  */
object SqlTest {

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
    val sc = new SparkSession
    .Builder()
      .appName("source_data_mysql001")
      .master("local")
      .getOrCreate()
    val url = "jdbc:mysql://192.168.1.100:3306/Strengthen?useUnicode=true&characterEncoding=utf8"


    val SC = "SC"
    val Student = "Student"
    val Teacher = "Teacher"
    val df_SC = sc.read.jdbc(url, SC, dbConnProperties())
    val df_Student = sc.read.jdbc(url, Student, dbConnProperties())
    val df_Teacher = sc.read.jdbc(url, Teacher, dbConnProperties())
    //df_SC.show() //使用一个action算子来检查是否能读取数据
    //df_Student.show()
    //df_Teacher.show()

    df_SC.createOrReplaceTempView("sc")
    df_Student.createOrReplaceTempView("stu")
    df_Teacher.createOrReplaceTempView("tc")
    //查询" 01 "课程比" 02 "课程成绩高的学生的信息及课程分数
    //    val frame1 = sc.sql("select * from sc s1 inner join SC s2 " +
    //      "on s1.SId = s2.SId " +
    //      "where s1.CId = '01' and s2.CId = '02' and s1.score > s2.score")
    //    frame1.show()

    //1.1 查询同时存在" 01 "课程和" 02 "课程的情况
    //    val frame = sc.sql("select * from sc s1 " +
    //      "where s1.SId in  " +
    //      "(select distinct(s2.SId) from sc s2 where s2.CId = '02') " +
    //      "and s1.CId = '01'")

    //    val frame = sc.sql(
    //      "select * from sc s1,sc s2 " +
    //        "where s1.SId = s2.SId " +
    //        "and s1.CId = '01' " +
    //        "and s2.CId = '02' " +
    //        "order by s1.SId")


    //    val frame = sc.sql("select * from sc s1 " +
    //      "where s1.SId not in " +
    //      "(select SId from sc where sc.CId = '01') " +
    //      "and s1.CId = '02' " +
    //      "order by s1.SId")

    //    val frame = sc.sql(
    //      "select *,Row_Number() over ( order by score) rank from SC where SId in('01','02') order by rank")


    //val frame = sc.sql("select stu.*,scavg.avg from stu join (select SId,avg(sc.score) as avg from sc group by SId having avg(sc.score) >= 60) scavg  on stu.SId = scavg.SId")

    //val frame = sc.sql("select distinct(stu.*) from sc join stu on sc.SId = stu.SId where score is not null")

    //val frame = sc.sql("select stu.* from stu inner join (select distinct(SID) as id from sc where score is not null) sc on stu.SId = sc.id")

    //val frame = sc.sql("select stu.*,s.count,s.sum from stu left join (select SId,count(DISTINCT CId) as count,sum(score) as sum from sc group by SId) as s on stu.SId = s.SId order by stu.SId")

    //    val frame1 = sc.sql("select * from sc where score like '8%'")
    //    frame1.show()
    //
    val frame = sc.sql("select * from tc where Tname like '李%'")



  }

}
