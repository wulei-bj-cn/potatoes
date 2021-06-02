//查询语句
select (orders.price * order.quantity) as revenue, users.name
from orders inner join users on orders.userId = users.id
where users.type = ‘Head User’
group by users.name
order by revenue desc


//查询语句，注意orders_new是分区表
select (orders_new.price * orders_new.quantity) as revenue, users.name
from orders_new inner join users on orders_new.userId = users.id
where users.type = ‘Head User’
group by users.name
order by revenue desc
