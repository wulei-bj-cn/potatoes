import org.apache.spark.util.SizeEstimator
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.concurrent.TimeUnit

object analysis {

  def time[T](func: => T): String = {
    val start = System.nanoTime()
    val result = func
    val end = System.nanoTime()
    val elapsedTime = TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS)
    "Elapsed time: " + elapsedTime + "ms"
  }

  def sizeOld[T](func: => T): T = {
    val size = SizeEstimator.estimate(func.asInstanceOf[DataFrame])
    val result = func
    println("Estimated size: " + size/1024 + "KB")
    result
  }

  def sizeNew(func: => DataFrame, spark: => SparkSession): String = {
    val result = func
    val lp = result.queryExecution.logical
    val size = spark.sessionState.executePlan(lp).optimizedPlan.stats.sizeInBytes
    "Estimated size: " + size/1024 + "KB"
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("License Plate Lottery")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val start = System.nanoTime()
    val sourceRoot: String = ""

    // 申请者数据（因为倍率的原因，每一期，同一个人，可能有多个号码）
    val hdfs_path_apply = s"${sourceRoot}/apply"
    val applyNumbersDF = spark.read.parquet(hdfs_path_apply)

    // 中签者数据
    val hdfs_path_lucky = s"${sourceRoot}/lucky"
    val luckyDogsDF = spark.read.parquet(hdfs_path_lucky)

    // 增加cache
    applyNumbersDF.cache()
    // 统计申请数（同一批次含重复编号）, 返回值：381972118
    println("apply num: %d".format(applyNumbersDF.count()))
    println("apply data size: %s".format(sizeNew(applyNumbersDF, spark)))
    /**
      场景一：人数统计，单表Shuffle
      以batchNum为粒度，对applyNumbersDF去重
      优化空间：是否加Cache
     */
    val applyDistinctDF = applyNumbersDF.select("batchNum", "carNum").distinct
    // 增加cache
    applyDistinctDF.cache()
    // 统计申请人次（同一批次不含重复编号）， 返回值：135009819，执行时间：50595ms
    println(s"Case 01: " + time(applyDistinctDF.write.format("noop").mode("overwrite").save()))

    //从2011年到2019年，总共有多少中签的幸运儿
    // 增加cache
    luckyDogsDF.cache()
    // 统计中签人数（不存在一人多号的问题），返回值：1150828
    println("lucky num: %d".format(luckyDogsDF.count()))
    println("lucky num data size: %s".format(sizeNew(luckyDogsDF, spark)))

    /**
      场景二：不同人群（未中签、已中签）的摇号次数（批次数），单表Shuffle、子查询
     */
    // 场景02_01：所有参与摇号（包括未中签、已中签）的人，摇号次数的统计分布
    /*
      返回数据：
        x_axis：摇号次数
        y_axis：申请人数
    */
    val result02_01 = applyDistinctDF.groupBy(col("carNum")).agg(count(col("carNum")).alias("x_axis"))
      .groupBy(col("x_axis")).agg(count("carNum").alias("y_axis"))
      .orderBy("x_axis")
    println(s"Case 02_01: " + time(result02_01.write.format("noop").mode("overwrite").save()))

    // 场景02_02：所有参与摇号（已中签）的人，摇号次数的统计分布
    /*
      返回数据：
        x_axis：摇号次数
        y_axis：中签人数
    */
    val result02_02 = applyDistinctDF
      .join(luckyDogsDF.select("carNum"), Seq("carNum"), "inner")
      .groupBy(col("carNum")).agg(count("carNum").alias("x_axis"))
      .groupBy(col("x_axis")).agg(count("carNum").alias("y_axis"))
      .orderBy("x_axis")
    println(s"Case 02_02: " + time(result02_02.write.format("noop").mode("overwrite").save()))

    /**
      场景三：统计2011-2019每一批次的摇号中签率，两表Join、分组、聚合
    返回数据：
      batchNum：批次
      denominator：申请者人数
      molecule：中签者人数
      ratio：中签比例
  */
    // 统计每批次申请者的人数
    val apply_denominator = applyDistinctDF.groupBy(col("batchNum")).agg(count("carNum").alias("denominator"))
    // 统计每批次中签者的人数
    val lucky_molecule = luckyDogsDF.groupBy(col("batchNum")).agg(count("carNum").alias("molecule"))
    println("lucky_molecule size: %s".format(sizeNew(lucky_molecule, spark)))
    val result03 = apply_denominator.join(lucky_molecule, Seq("batchNum"), "inner").withColumn("ratio", round(col("molecule")/col("denominator"), 5)).orderBy("batchNum")
    println(s"Case 03: " + time(result03.write.format("noop").mode("overwrite").save()))

    /**
      场景四：统计2018每一批次摇号中签率，两表Join、分组、聚合
      优化空间：AQE的Join策略调整、DPP。
     返回数据：
        batchNum：批次
        denominator：申请者人数
        molecule：中签者人数
        ratio：中签比例
     */
    // 筛选出2018年的中签数据，并按照批次统计中签人数
    val lucky_molecule_2018 = luckyDogsDF.filter(col("batchNum").like("2018%")).groupBy(col("batchNum")).agg(count("carNum").alias("molecule"))
    // 通过与筛选出的中签数据按照批次关联，以获取每个批次对应的申请人数，并计算每期的中签率
    val result04 = apply_denominator.join(lucky_molecule_2018, Seq("batchNum"), "inner").withColumn("ratio", round(col("molecule")/col("denominator"), 5)).orderBy("batchNum")
    println(s"Case 04: " + time(result04.write.format("noop").mode("overwrite").save()))

    /**
      场景五：统计2016-2019中签者的倍率分布，两表Join、分组、聚合、过滤
      因为从2016年开始，才开始都有倍率，所以在本场景中分析的数据是从2016年第一期开始
      优化空间：AQE的Join策略调整、DPP
      目的：看看倍率这个东西，到底是有用还是没用？
     */
    // 场景05_01: 求出各个申请倍率下的中签人数
    /*
      返回数据：
        multiplier：倍数
        cnt：中签者人数
    */
    // 过滤出2016-2019申请者数据，统计出每个申请者在每一期内的倍率，并在所有批次中选取最大的倍率作为申请者的最终倍率，最终算出各个倍率下的申请人数
    val result05_01 = applyNumbersDF.join(luckyDogsDF.filter(col("batchNum") >= "201601").select("carNum"), Seq("carNum"), "inner")
      .groupBy(col("batchNum"), col("carNum")).agg(count(lit(1)).alias("multiplier"))
      .groupBy("carNum").agg(max("multiplier").alias("multiplier"))
      .groupBy("multiplier").agg(count(lit(1)).alias("cnt")).orderBy("multiplier")
    println(s"Case 05_01: " + time(result05_01.write.format("noop").mode("overwrite").save()))

    // 场景05_02: 求出各个申请倍率下的中签人数与申请者人数，并计算中签率
    /*
      返回数据：
        multiplier：倍数
        apply_cnt：申请者人数
        lucy_cnt：中签者人数
        ratio：中签比例
    */
    // Step01: 过滤出2016-2019申请者数据，统计出每个申请者在每一期内的倍率，并在所有批次中选取最大的倍率作为申请者的最终倍率，最终算出各个倍率下的申请人数
    val apply_multiplier_2016_2019 = applyNumbersDF.filter(col("batchNum") >= "201601")
      .groupBy(col("batchNum"), col("carNum")).agg(count(lit(1)).alias("multiplier"))
      .groupBy("carNum").agg(max("multiplier").alias("multiplier"))
      .groupBy("multiplier").agg(count(lit(1)).alias("apply_cnt"))
    // Step02: 将各个倍率下的申请人数与各个倍率下的中签人数左关联，并求出各个倍率下的中签率
    val result05_02 = apply_multiplier_2016_2019
      .join(result05_01.withColumnRenamed("cnt", "lucy_cnt"), Seq("multiplier"), "left").na.fill(0)
      .withColumn("ratio", round(col("lucy_cnt")/col("apply_cnt"), 5))
      .orderBy("multiplier")
    println(s"Case 05_02: " + time(result05_02.write.format("noop").mode("overwrite").save()))

  }
}

