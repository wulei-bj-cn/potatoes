import java.security.MessageDigest
 
//反例代码

class Util {
val md5: MessageDigest = MessageDigest.getInstance("MD5")
val sha256: MessageDigest = _ //其他哈希算法
}
 
val df: DataFrame = _
val ds: Dataset[Row] = df.map{
case row: Row =>
val util = new Util()
val s: String = row.getString(0) + row.getString(1) + row.getString(2)
val hashKey: String = util.md5.digest(s.getBytes).map("%02X".format(_)).mkString
(hashKey, row.getInt(3))
}

//正例代码

val ds: Dataset[Row] = df.mapPartitions(iterator => {
val util = new Util()
val res = iterator.map{
case row=>{
val s: String = row.getString(0) + row.getString(1) + row.getString(2)
val hashKey: String = util.md5.digest(s.getBytes).map("%02X".format(_)).mkString
(hashKey, row.getInt(3)) }}
res
})
