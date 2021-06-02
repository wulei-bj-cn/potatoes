//查询语句
select (orders.price * order.quantity) as revenue, users.name
from orders inner join users on orders.userId = users.id
group by users.name
order by revenue desc


//添加Join hints之后的查询语句
select /*+ shuffle_hash(orders) */ (orders.price * order.quantity) as revenue, users.name
from orders inner join users on orders.userId = users.id
group by users.name
order by revenue desc
