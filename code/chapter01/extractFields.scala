//实现方案1 —— 反例
val extractFields: Seq[Row] => Seq[(String, Int)] = { 
  (rows: Seq[Row]) => { 
    var fields = Seq[(String, Int)]() 
    rows.map(row => { 
      fields = fields :+ (row.getString(2), row.getInt(4)) 
    }) 
  fields 
  }
}

//实现方案2 —— 正例
val extractFields: Seq[Row] => Seq[(String, Int)] = { 
  (rows: Seq[Row]) => 
    rows.map(row => (row.getString(2), row.getInt(4))).toSeq
}
