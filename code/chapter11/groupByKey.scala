val flowers = spark.sparkContext.textFile("flowers.txt")
//黄老师给5个小同学分发花朵
val flowersForKids = flowers.coalesce(5)
val flowersKV = flowersForKids.map((_, 1))
//黄小乙的两个步骤：大家先各自按颜色归类，然后再把归类后的花朵放到相应的课桌上 
flowersKV.groupByKey.collect

