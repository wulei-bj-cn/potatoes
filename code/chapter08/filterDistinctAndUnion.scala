//反例代码

val dates: List[String] = List("2020-01-01", "2020-01-02", "2020-01-03")
val rootPath: String = _
 
//读取日志文件，去重、并展开userInterestList
def createDF(rootPath: String, date: String): DataFrame = {
val path: String = rootPath + date
val df = spark.read.parquet(path)
.distinct
.withColumn("userInterest", explode(col("userInterestList")))
df
}
 
//提取字段、过滤，再次去重，把多天的结果用union合并
val distinctItems: DataFrame = dates.map{
case date: String =>
val df: DataFrame = createDF(rootPath, date)
.select("userId", "itemId", "userInterest", "accessFreq")
.filter("accessFreq in ('High', 'Medium')")
.distinct
df
}.reduce(_ union _)

//正例代码

val dates: List[String] = List("2020-01-01", "2020-01-02", "2020-01-03")
val rootPath: String = _
 
val filePaths: List[String] = dates.map(rootPath + _)
 
/**
一次性调度所有文件
先进行过滤和列剪枝
然后再展开userInterestList
最后统一去重
*/
val distinctItems = spark.read.parquet(filePaths: _*)
.filter("accessFreq in ('High', 'Medium'))")
.select("userId", "itemId", "userInterestList")
.withColumn("userInterest", explode(col("userInterestList")))
.select("userId", "itemId", "userInterest")
.distinct
