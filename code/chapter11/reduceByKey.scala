val flowers = spark.sparkContext.textFile("flowers.txt")
//黄老师给5个小同学分发花朵
val flowersForKids = flowers.coalesce(5)
val flowersKV = flowersForKids.map((_, 1))
//黄小乙的两个步骤：大家先各自按颜色计数，然后再按照课桌统一计数 
flowersKV.reduceByKey(_ + _).collect

