val filePath: String = _
val df = spark.read.parquent(filePath)
df.groupBy(“groupId”)
.agg(array_distinct(flatten(collect_list(col(“interestList”)))))
