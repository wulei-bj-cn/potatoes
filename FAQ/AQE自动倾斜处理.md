#### 提问：开启AQE之后，开发者就不用手动处理数据倾斜、完全地扔给Spark SQL是嘛？

> 解答：在 Reduce 阶段，当 Reduce Task 所需处理的分区尺寸大于一定阈值时，利用 OptimizeSkewedJoin 策略，AQE 会把大分区拆成多个小分区。
> 
> 在同一个 Executor 内部，本该由一个 Task 去处理的大分区，被 AQE 拆成多个小分区并交由多个 Task 去计算。这样一来，Task 之间的计算负载就可以得到平衡。但是，这并不能解决不同 Executors 之间的负载均衡问题。
> 
> 如果倾斜问题存在于Executors之间，那么我们就需要“两阶段Shuffle”来将数据打散，从而平衡Executors之间的工作负载。“两阶段Shuffle”的实现过程，可以参考第29讲`大表Join大表（二）：什么是负隅顽抗的调优思路？`

*贡献来源*：`布兰特`
