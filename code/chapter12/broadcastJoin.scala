import org.apache.spark.sql.functions.broadcast
 
val transactionsDF: DataFrame = _
val userDF: DataFrame = _
 
val bcUserDF = broadcast(userDF)
transactionsDF.join(bcUserDF, Seq(“userID”), “inner”)

