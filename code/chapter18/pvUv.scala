package com.geek.spark666.universal3

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 18 | 磁盘视角：如果内存无限大，磁盘还有用武之地吗？
  */
object pvUv {
  import org.apache.spark.sql.functions._
  val clsName: String = this.getClass.getSimpleName
  val appName: String = clsName.substring(0,clsName.length-1)
  val MasterLocalMax = "local[4]"


  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
            .appName(appName)
            .master(MasterLocalMax)
            .getOrCreate()

    //todo 文件路径根据你的具体情况而定
    val filePath: String = ".\\data_in\\18\\18"

    //反例
    val resultDF = compute1(session,filePath)

    //正例
    //val resultDF = compute2(session,filePath)

    /**
      * Result样例
      * | userId | metrics | value |
      * | user0  | PV      | 25    |(一共浏览了25次页面)
      * | user0  | UV      | 12    |(其中,共有12个不同的页面)
      */
    resultDF.show()
    //resultDF.explain()

    //线程暂停, 查看sparkUI
    Thread.sleep(1000000000L)
  }

  /**
    * 版本1：分别计算PV、UV，然后合并
    * Data schema (userId: String, accessTime: Timestamp, page: String)
    */
  def compute1(session: SparkSession,filePath: String)={
    val df: DataFrame = session.read.json(filePath)

    val dfPV: DataFrame = df.groupBy("userId")
            .agg(count("page").alias("value"))//表示一共浏览了多少次页面(不去重)
            .withColumn("metrics", lit("PV"))

    val dfUV: DataFrame = df.groupBy("userId")
            .agg(countDistinct("page").alias("value"))//表示一共浏览了多少个页面(去重)
            .withColumn("metrics ", lit("UV"))

    dfPV.union(dfUV)
  }

  /**
    * 版本2：分别计算PV、UV，然后合并
    *  Data schema (userId: String, accessTime: Timestamp, page: String)
    *  在用 Parquet API 读取用户日志之后，我们追加一步重分区操作，也就是以 userId 为分区键调用 repartition 算子。
    */
  def compute2(session: SparkSession,filePath: String)={
    val df = session.read.json(filePath)
            .repartition(column("userId"))

    val dfPV: DataFrame = df.groupBy("userId")
            .agg(count("page").alias("value"))
            .withColumn("metrics", lit("PV"))

    val dfUV: DataFrame = df.groupBy("userId")
            .agg(countDistinct("page").alias("value"))
            .withColumn("metrics ", lit("UV"))

    dfPV.union(dfUV)
  }



}
