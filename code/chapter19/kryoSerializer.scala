//向Kryo Serializer注册类型
val conf = new SparkConf().setMaster(“”).setAppName(“”)
conf.registerKryoClasses(Array(
classOf[Array[String]],
classOf[HashMap[String, String]],
classOf[MyClass]
))
