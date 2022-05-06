
// this code runs the merged data from nft and ether price data, then merge it with twitter data
////////read twitter data scala
val df = spark.read.option("inferSchema", "true").csv("out3.csv")

//add column name
val newFrame =  df.withColumnRenamed("_c0", "dates").withColumnRenamed("_c1","tweets")

//preparing for merge
val df3 = newFrame.selectExpr("cast(dates as string) dates", "cast(tweets as int) tweets")

//turn timestamp data to date data
val df4 = df3.withColumn("dates",date_format(to_date(col("dates"), "MM/dd/yy"), "yyyy-MM-dd"))

//load new data
val df5=spark.read.option("inferSchema", "true").option("header", "true").csv("merged.csv")

val df6 = df5.withColumn("Date",date_format(to_date(col("Date"), "MM/dd/yy"), "yyyy-MM-dd"))

df4.createOrReplaceTempView("df4")

df6.createOrReplaceTempView("df6")

//merge data on the same date
var merged = spark.sql("SELECT * FROM df4 INNER JOIN df6 ON df4.dates = df6.Date")

merged.createOrReplaceTempView("merged")

merged.drop("dates")

merged.coalesce(1).write.option("header","true").csv("merged_4.csv")
//df5 merge with df4

