//以date字段拆分内表
val query: String = "
select sum(tx.price * tx.quantity) as revenue, o.orderId
from transactions as tx inner join orders as o
on tx.orderId = o.orderId
where o.status = 'COMPLETE'
and o.date = '2020-01-01'
group by o.orderId
"

//循环遍历dates、完成“分而治之”的计算
val dates: Seq[String] = Seq("2020-01-01", "2020-01-02", … "2020-03-31")
 
for (date <- dates) {
 
val query: String = s"
select sum(tx.price * tx.quantity) as revenue, o.orderId
from transactions as tx inner join orders as o
on tx.orderId = o.orderId
where o.status = 'COMPLETE'
and o.date = ${date}
group by o.orderId
"
 
val file: String = s"${outFile}/${date}"
spark.sql(query).save.parquet(file)
}
