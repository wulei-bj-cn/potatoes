//统计订单交易额的代码实现
val txFile: String = _
val orderFile: String = _
 
val transactions: DataFrame = spark.read.parquent(txFile)
val orders: DataFrame = spark.read.parquent(orderFile)
 
transactions.createOrReplaceTempView(“transactions”)
orders.createOrReplaceTempView(“orders”)
 
val query: String = “
select sum(tx.price * tx.quantity) as revenue, o.orderId
from transactions as tx inner join orders as o
on tx.orderId = o.orderId
where o.status = ‘COMPLETE’
and o.date between ‘2020-01-01’ and ‘2020-03-31’
group by o.orderId
”
 
val outFile: String = _
spark.sql(query).save.parquet(outFile)

