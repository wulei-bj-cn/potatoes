val dict: List[String] = List(“spark”, “scala”)
val words: RDD[String] = sparkContext.textFile(“~/words.csv”)
val keywords: RDD[String] = words.filter(word => dict.contains(word))
keywords.cache
keywords.count
keywords.map((_, 1)).reduceByKey(_ + _).collect
