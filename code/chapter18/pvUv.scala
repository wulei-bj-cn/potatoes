//版本1：分别计算PV、UV，然后合并
// Data schema (userId: String, accessTime: Timestamp, page: String)
 
val filePath: String = _
val df: DataFrame = spark.read.parquet(filePath)
 
val dfPV: DataFrame = df.groupBy("userId").agg(count("page").alias("value")).withColumn("metrics", lit("PV"))
val dfUV: DataFrame = df.groupBy("userId").agg(countDistinct("page").alias("value")).withColumn("metrics ", lit("UV"))
 
val resultDF: DataFrame = dfPV.Union(dfUV)
 
// Result样例
| userId | metrics | value |
| user0  | PV      | 25 |
| user0  | UV      | 12 |


//版本2：分别计算PV、UV，然后合并
// Data schema (userId: String, accessTime: Timestamp, page: String)
 
val filePath: String = _
val df: DataFrame = spark.read.parquet(filePath).repartition($"userId")
 
val dfPV: DataFrame = df.groupBy("userId").agg(count("page").alias("value")).withColumn("metrics", lit("PV"))
val dfUV: DataFrame = df.groupBy("userId").agg(countDistinct("page").alias("value")).withColumn("metrics ", lit("UV"))
 
val resultDF: DataFrame = dfPV.Union(dfUV)
 
// Result样例
| userId | metrics | value |
| user0  | PV      | 25 |
| user0  | UV      | 12 |
