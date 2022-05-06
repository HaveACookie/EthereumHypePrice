// This code Profiles twitter data, and then merges the data together into one dataframe!
//This builds on profiling that was done in raw map reduce as well that is in the MR folder
//load twitter data (which comes as a mapreduce file)
val df1 = spark.read.text("final/cleanSource/output/part-r-00000")
//add column name
val newFrame1 =df1.withColumn("dates", split(col("value"),"\\s+").getItem(0).as("cleanDate")).withColumn("tweets", split(col("value"),"\\s+").getItem(1).as("cleanDate"))
//cast as dates and int for merge
val df3 = newFrame1.selectExpr("cast(dates as string) dates", "cast(tweets as int) tweets")
//convert dates to a standard format more consistent with the other files
val df4 = df3.withColumn("dates",date_format(to_date(col("dates"), "yyyy/MM/d"), "yyyy-MM-dd"))
df4.createOrReplaceTempView("df4")
//average tweets from cleaned tweet data
spark.sql("SELECT AVG(Tweets) FROM df4").show()
// +------------------+
// |       avg(Tweets)|
// +------------------+
// |12767.716341689878|
// +------------------+
//count  of tweets from cleaned tweet data
spark.sql("SELECT count(Tweets) FROM df4").show()
//+-------------+
// |count(Tweets)|
// +-------------+
// |         2154|
// +-------------+
//min tweets from cleaned tweet data
spark.sql("SELECT min(Tweets) FROM df4").show()
//Read Second Data file
// +-----------+
// |min(Tweets)|
// +-----------+
// |        543|
// +-----------+


df4.describe().show()
// +-------+----------+------------------+
// |summary|     dates|            tweets|
// +-------+----------+------------------+
// |  count|      2154|              2154|
// |   mean|      null|12767.716341689878|
// | stddev|      null|12674.528669603547|
// |    min|2016-03-16|               543|
// |    max|2022-04-02|            136628|
// +-------+----------+------------------+
val df2 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("cleanSource/output/nftData.csv")
//convert dates to a standard format more consistent with the other files
val df5 = df2.withColumn("DateTime", split(col("DateTime"),"\\s+").getItem(0).as("cleanDate"))
// Renames Sales name to shorter name more compatible with SPARK SQL queries
val df6 = df5.withColumnRenamed("Sales (USD) (y)", "Sales")
df6.createOrReplaceTempView("df6")
spark.sql("SELECT DateTime, Sales FROM df6")
df4.createOrReplaceTempView("df4")
// spark.sql("SELECT")
// spark.sql("SELECT * FROM df4 INNER JOIN df6 ON df4.dates = df6.DateTime")

val tweetsSales = spark.sql("SELECT * FROM df4 INNER JOIN df6 ON df4.dates = df6.DateTime")

val df9 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("cleanSource/output/price.csv")

val df10 = df9.withColumn("Date", date_format(to_date(col("Date"), "MM/dd/yy"), "yyyy-MM-dd"))

df10.createOrReplaceTempView("df10")

tweetsSales.createOrReplaceTempView("merger")

val merged = spark.sql("SELECT * FROM df10 INNER JOIN merger ON df10.Date = merger.dates")

//merged has everything
merged.createOrReplaceTempView("MainFrame")

val dailyAverages = spark.sql(" SELECT Date, (High+Low)/2 AS DailyAverage FROM MainFrame")

dailyAverages.createOrReplaceTempView("dailyAverages")

//Add the daily average for every column that can get one to the central dataframe
spark.sql("SELECT dailyAverages.DailyAverage, MainFrame.* FROM dailyAverages INNER JOIN MainFrame ON dailyAverages.Date = MainFrame.Date")
val newFrame = spark.sql("SELECT MainFrame.*, dailyAverages.DailyAverage FROm MainFrame LEFT JOIN dailyAverages ON MainFrame.Date = dailyAverages.Date ORDER BY Date")
newFrame.createOrReplaceTempView("newMain")
//Export current version
val mergetemp = spark.sql("SELECT Date, DailyAverage as Price, Sales FROM newMain")
val mergetemp2 = spark.sql("SELECT DateTime as Date, DailyAverage, Sales, Tweets FROM newMain")
mergetemp.coalesce(1).write.option("header","true").csv("merged.csv")
mergetemp2.coalesce(1).write.option("header","true").csv("merged2.csv")
//export to files for usage.
val newFrame = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("merged2.csv")
newFrame.createOrReplaceTempView("newMain")
//Get the average Daily price value
spark.sql("SELECT AVG(DailyAverage) FROM newMain").show()
// +------------------+
// | avg(DailyAverage)|
// +------------------+
// |1322.0268310546883|
// +------------------+
//Get the average number of Tweets
//What is the Average of the Tweet Data?
spark.sql("SELECT AVG(Tweets) FROM newMain").show()
// +----------------+
// |     avg(Tweets)|
// +----------------+
// |15220.5673828125|
// +----------------+

//Find total number of records with the result of running

// what is the minimum number of tweets
spark.sql("SELECT min(Tweets) FROM newMain").show()