
/////data profiling:
//first, run the cleaning scala file from cleaning repository
//this would give us a dataframe of just two columns

var nft = spark.read.option("inferSchema", "true").csv("nft_v1.csv")
nft=nft.withColumnRenamed("_c0","time")
nft=nft.withColumnRenamed("_c1","nft_sales")
nft.createOrReplaceTempView("nft")

nft=nft.withColumn("time", regexp_replace($"time", "\\s+", ""))

// scala> nft.schema
// res2: org.apache.spark.sql.types.StructType = StructType(StructField(time,StringType,true), StructField(nft_sales,StringType,true))

val df=nft.filter(nft.col("time")=!="DateTime")

val df1 = df.selectExpr("cast(time as DATE) time", "cast(nft_sales as double) nft_sales")

df1.createOrReplaceTempView("df1")

//what is the average of the data
spark.sql("SELECT AVG(nft_sales) FROM df1").show()

// scala> spark.sql("SELECT AVG(nft_sales) FROM nft").show()
// +------------------------------+
// |avg(CAST(nft_sales AS DOUBLE))|
// +------------------------------+
// |          1.7745630045730967E7|
// +------------------------------+

// What is the total number of records in each data source?
spark.sql("SELECT count(nft_sales) FROM df1").show()

// scala> spark.sql("SELECT count(nft_sales) FROM nft").show()
// +----------------+
// |count(nft_sales)|
// +----------------+
// |            1710|
// +----------------+


spark.sql("SELECT min(time) FROM df1").show()
// scala> spark.sql("SELECT min(time) FROM df").show()
// +----------+
// | min(time)|
// +----------+
// |2017-06-23|
// +----------+

spark.sql("SELECT max(time) FROM df1").show()
// +----------+                                                                    
// | max(time)|
// +----------+
// |2022-04-07|
// +----------+

df1.describe().show()
// scala> df1.describe().show()
// +-------+--------------------+
// |summary|           nft_sales|
// +-------+--------------------+
// |  count|                1710|
// |   mean|1.7745630045730967E7|
// | stddev| 4.335849625516924E7|
// |    min|                21.3|
// |    max|      4.6334542738E8|
// +-------+--------------------+

////number of days when sales if above average
spark.sql("SELECT COUNT(time) FROM df1 where nft_sales>1.7745630045730967E7").show()

// scala> spark.sql("SELECT COUNT(time) FROM df1 where nft_sales>1.7745630045730967E7").show()
// +-----------+                                                                   
// |count(time)|
// +-----------+
// |        295|
// +-----------+

//it means that there is a high variance in our data: most of the days it is very low, and on some days
//it is very high

