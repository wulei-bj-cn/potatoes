//利用noop精确计算DataFrame运行时间
df.write
.format(“noop”)
.save()
