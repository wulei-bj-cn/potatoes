//Prepare data
val table1: DataFrame = spark.read.parquet(path1)
val table2: DataFrame = spark.read.parquet(path2)
table1.createOrReplaceTempView("t1")
table2.createOrReplaceTempView("t2")
 
//Force broadcast by join hints in SQL
val query: String = “select /*+ broadcast(t2) */ * from t1 inner join t2 on t1.key = t2.key”
val queryResutls: DataFrame = spark.sql(query)

//Force broadcast by join hints in DataFrame
table1.join(table2.hint(“broadcast”), Seq(“key”), “inner”)

//Force broadcast by function
import org.apache.spark.sql.functions.broadcast
table1.join(broadcast(table2), Seq(“key”), “inner”)
