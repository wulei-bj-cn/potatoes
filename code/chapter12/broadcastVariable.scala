val dict = List(“spark”, “tune”)
val bc = spark.sparkContext.broadcast(dict)
val words = spark.sparkContext.textFile(“~/words.csv”)
val keywords = words.filter(word => bc.value.contains(word))
keywords.map((_, 1)).reduceByKey(_ + _).collect
