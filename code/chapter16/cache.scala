val filePath: String = _
val df: DataFrame = spark.read.parquet(filePath)
 
//Cache方式一
val cachedDF = df.cache
//数据分析
cachedDF.filter(col2 > 0).select(col1, col2)
cachedDF.select(col1, col2).filter(col2 > 100)
 
//Cache方式二
df.select(col1, col2).filter(col2 > 0).cache
//数据分析
df.filter(col2 > 0).select(col1, col2)
df.select(col1, col2).filter(col2 > 100)
 
//Cache方式三
val cachedDF = df.select(col1, col2).cache
//数据分析
cachedDF.filter(col2 > 0).select(col1, col2)
cachedDF.select(col1, col2).filter(col2 > 100)

