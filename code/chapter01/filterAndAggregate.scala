/** (startDate, endDate) 
  e.g. ("2021-01-01", "2021-01-31")
*/

val pairDF: DataFrame = _ 

/** (dim1, dim2, dim3, eventDate, value)
  e.g. ("X", "Y", "Z", "2021-01-15", 12)
*/

val factDF: DataFrame = _ 
// Storage root path
val rootPath: String = _

//实现方案1 —— 反例
def createInstance(factDF: DataFrame, startDate: String, endDate: String): DataFrame = {
  val instanceDF = factDF
                     .filter(col("eventDate") > lit(startDate) && col("eventDate") <= lit(endDate))
                     .groupBy("dim1", "dim2", "dim3", "event_date")
                     .agg(sum("value") as "sum_value")
  instanceDF
} 

pairDF.collect.foreach {
  case (startDate: String, endDate: String) =>
    val instance = createInstance(factDF, startDate, endDate)
    val outPath = s"${rootPath}/endDate=${endDate}/startDate=${startDate}"
    instance.write.parquet(outPath)
}

//实现方案2 —— 正例
val instances = factDF
                  .join(pairDF, factDF("eventDate") > pairDF("startDate") && factDF("eventDate") <= pairDF("endDate"))
                  .groupBy("dim1", "dim2", "dim3", "eventDate", "startDate", "endDate")
                  .agg(sum("value") as "sum_value") 

instances.write.partitionBy("endDate", "startDate").parquet(rootPath)
