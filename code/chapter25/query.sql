select (orders.price * order.quantity) as income, users.name
from orders inner join users on orders.userId = users.id
where users.type = ‘Head User’
group by users.name
order by income desc
