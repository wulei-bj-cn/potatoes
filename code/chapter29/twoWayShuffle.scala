//根据Join Keys是否倾斜、将内外表分别拆分为两部分
import org.apache.spark.sql.functions.array_contains
 
//将Join Keys分为两组，存在倾斜的、和分布均匀的
val skewOrderIds: Array[Int] = _
val evenOrderIds: Array[Int] = _
 
val skewTx: DataFrame = transactions.filter(array_contains(lit(skewOrderIds),$"orderId"))
val evenTx: DataFrame = transactions.filter(array_contains(lit(evenOrderIds),$"orderId"))
 
val skewOrders: DataFrame = orders.filter(array_contains(lit(skewOrderIds),$"orderId"))
val evenOrders: DataFrame = orders.filter(array_contains(lit(evenOrderIds),$"orderId"))

//将分布均匀的数据分别注册为临时表
evenTx.createOrReplaceTempView(“evenTx”)
evenOrders.createOrReplaceTempView(“evenOrders”)
 
val evenQuery: String = “
select /*+ shuffle_hash(orders) */ sum(tx.price * tx.quantity) as revenue, o.orderId
from evenTx as tx inner join evenOrders as o
on tx.orderId = o.orderId
where o.status = ‘COMPLETE’
and o.date between ‘2020-01-01’ and ‘2020-03-31’
group by o.orderId
”
val evenResults: DataFrame = spark.sql(evenQuery)

import org.apache.spark.sql.functions.udf
 
//定义获取随机盐粒的UDF
val numExecutors: Int = _
val rand = () => scala.util.Random.nextInt(numExecutors)
val randUdf = udf(rand)
 
//第一阶段的加盐操作。注意：保留orderId字段，用于后期第二阶段的去盐化
 
//外表随机加盐
val saltedSkewTx = skewTx.withColumn(“joinKey”, concat($“orderId”, lit(“_”), randUdf()))
 
//内表复制加盐
var saltedskewOrders = skewOrders.withColumn(“joinKey”, concat($“orderId”, lit(“_”), lit(1)))
for (i <- 2 to numExecutors) {
saltedskewOrders = saltedskewOrders union skewOrders.withColumn(“joinKey”, concat($“orderId”, lit(“_”), lit(i)))
}

//将加盐后的数据分别注册为临时表
saltedSkewTx.createOrReplaceTempView(“saltedSkewTx”)
saltedskewOrders.createOrReplaceTempView(“saltedskewOrders”)
 
val skewQuery: String = “
select /*+ shuffle_hash(orders) */ sum(tx.price * tx.quantity) as initialRevenue, o.orderId, o.joinKey
from saltedSkewTx as tx inner join saltedskewOrders as o
on tx.joinKey = o.joinKey
where o.status = ‘COMPLETE’
and o.date between ‘2020-01-01’ and ‘2020-03-31’
group by o.joinKey
”
//第一阶段加盐、Shuffle、关联、聚合后的初步结果
val skewInitialResults: DataFrame = spark.sql(skewQuery)

val skewResults: DataFrame = skewInitialResults.select(“initialRevenue”, “orderId”)
.groupBy(col(“orderId”)).agg(sum(col(“initialRevenue”)).alias(“revenue”))

evenResults union skewResults
