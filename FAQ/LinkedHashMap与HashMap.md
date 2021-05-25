#### 提问：MemoryStore为什么没有使用普通的HashMap，而是采用了更加复杂的LinkedHashMap来维护缓存列表

> 解答：LinkedHashMap也是一种HashMap，它的特别之处在于，其在内部用一个双向链表来维护键值对的顺序，每个键值对同时存储在哈希表、和双向链表中。

> **我们来看LinkedHashMap的特性：**

> - [x] 插入有序，这个好理解，就是在链表后追加元素
> - [x] 访问有序，这个就厉害了。访问有序说的是，对一个kv操作，不管put还是get，元素都会被挪到链表的末尾

> 在Spark RDD Cache的场景下，第一个特性不重要，重要的是第二个特性。当Storage Memory不足，Spark需要删除RDD Cache的时候，遵循的是LRU。那么问题来了，Spark是怎么实现的LRU的呢？
> 
> 答案就是它充分利用LinkedHashMap第二个特性，啥也不用做，就轻松地做到了这一点。根据LinkedHashMap访问有序的特性，最近访问过的元素都会依次挪到链表末尾，那么从链表表头开始，依次往后遍历，就都是那些“最近最少访问”的倒霉蛋。因此，当需要删除RDD Cache Block的时候，只需要从前往后依次删掉链表元素及其对应的Block就好了。

*贡献来源*：`超级达达`
