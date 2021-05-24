#### 提问：RDD与DataFrame的本质区别

> 解答：可以从4个方面来看

> - [x] 首先，从基本定义来看，RDD是不带Schema的分布式数据集，而DataFrame是带数据模式的结构化分布式数据集。核心区别在于DataFrame带数据模式，更像是传统DBMS中的一张表，RDD不带数据模式或者说是泛型的。

> - [x] 其次，从开发API来看，在形式上，RDD API中的高阶算子居多，而DataFrame API主要提供标量算子。再者，两者的底层优化引擎完全不同，RDD API的优化引擎是Spark Core，而DataFrame API的优化引擎是SparkSQL，其中包括Catalyst优化器和Tungsten。

> - [x] 再者，从地位来说，在DataFrame出现之前，Spark所有子框架、包括PySpark，全部基于RDD API实现。在DataFrame出现之后，上述子框架如Streaming、ML、Graph以及PySpark，全部由DataFrame API重写，目的在于共享Spark SQL带来的性能红利。

> - [x] 最后，再说说两者之间的联系。相比RDD API，尽管DataFrame API有众多优势，不过，DataFrame经过Spark SQL（Catalyst + Tungsten）优化过后，最终会转化成RDD[InternalRow]，并交由调度系统去调度、执行。为了方便理解和记忆，可以认为DataFrame API + SparkSQL的优化过程，目的就在于弥补之前的RDD API + Spark Core优化空间不足的缺陷。

**一句话概括，DataFrame API为开发者提供了更简单的API，让Spark做统一优化，从而让RDD在运行时的计算更加高效。**

*贡献来源*：`Fendora范东_`
