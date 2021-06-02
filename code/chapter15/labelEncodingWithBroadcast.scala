/**
广播变量实现方式
*/
//定义广播变量
val source = Source.fromFile(filePath, "UTF-8")
val lines = source.getLines().toArray
source.close()
val searchMap = lines.zip(0 until lines.size).toMap
val bcSearchMap = sparkSession.sparkContext.broadcast(searchMap)
 
//在Dataset中访问广播变量
bcSearchMap.value.getOrElse("体育-篮球-NBA-湖人", -1)

