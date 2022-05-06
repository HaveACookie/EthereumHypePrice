//Import File
val newFrame = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("merged2.csv")
newFrame.createOrReplaceTempView("newMain")

//What is the overall average Price
spark.sql("SELECT AVG(DailyAverage) FROM newMain").show()
// +------------------+
// | avg(DailyAverage)|
// +------------------+
// |1322.0268310546883|
// +------------------+
//order by average price per day
spark.sql("SELECT * from newMain ORDER BY DailyAverage DESC ").show()

//average tweets for days with below average price, which was derived from Data Profiler
spark.sql("SELECT avg(tweets) From (SELECT Date, DailyAverage, Tweets from newMain WHERE DailyAverage < 1322.0268310546883)").show()
// +-----------------+
// |      avg(tweets)|
// +-----------------+
// |5615.977419354838|
// +-----------------+
//average tweets for days with above average price
spark.sql("SELECT avg(tweets) From (SELECT Date, DailyAverage, Tweets from newMain WHERE DailyAverage > 1322.0268310546883)").show()
// +------------------+
// |       avg(tweets)|
// +------------------+
// |29960.284653465347|
// +------------------+
//So with this we can conclude that there are more tweets on high price days

//GET AVERAGE Sales(IN DOLLARS) 
spark.sql("SELECT AVG(Sales) FROM newMain").show()
// +-------------------+
// |         avg(Sales)|
// +-------------------+
// |2.669923217561522E7|
// +-------------------+
//This is equal to 26699232.17561522
//GET AVERAGE TWEETS for days above average number of sales
spark.sql("SELECT AVG(tweets) FROM (SELECT Date, Sales, Tweets FROM newMain WHERE Sales > 26699232.17561522)").show()
// +------------------+
// |       avg(tweets)|
// +------------------+
// |35273.237704918036|
// +------------------+
//get average tweets for days with below average number of sales
spark.sql("SELECT AVG(tweets) FROM (SELECT Date, Sales, Tweets FROM newMain WHERE Sales < 26699232.17561522)").show()
// +-----------------+
// |      avg(tweets)|
// +-----------------+
// |8947.680769230768|
// +-----------------+
//This shows that there are generally more tweets on average when there are more sales

//Get the average sales volume from days when price of ETH is below average
spark.sql("SELECT avg(Sales) From (SELECT Date, DailyAverage, Sales, Tweets from newMain WHERE DailyAverage < 1322.0268310546883)").show()
// |       avg(Sales)|
// +-----------------+
// |99062.80658064516|
// +-----------------+
//Get the average sales volume from days when price of ETH is above average
spark.sql("SELECT avg(Sales) From (SELECT Date, DailyAverage, Sales, Tweets from newMain WHERE DailyAverage > 1322.0268310546883)").show()
// +-------------------+
// |         avg(Sales)|
// +-------------------+
// |6.752127427660888E7|
// +-------------------+
// This shows that the sales volume is usually lower when the price is lower

//Get average tweet number
spark.sql("SELECT AVG(Tweets) FROM newMain").show()
// +----------------+
// |     avg(Tweets)|
// +----------------+
// |15220.5673828125|
// +----------------+

//Get Average NFT sales volume from days when the number of tweets is below average
 spark.sql("SELECT AVG(Sales) FROM (SELECT Date, Sales, Tweets FROM newMain WHERE Tweets < 15220.5673828125)").show()
// +-----------------+
// |       avg(Sales)|
// +-----------------+
// |908342.9714133737|
// +-----------------+
//Get Average NFT sales volume from days when the number of tweets is above average
spark.sql("SELECT AVG(Sales) FROM (SELECT Date, Sales, Tweets FROM newMain WHERE Tweets > 15220.5673828125)").show()
// +-------------------+
// |         avg(Sales)|
// +-------------------+
// |7.306645921486336E7|
// +-------------------+
//This shows that sales are higher on average on days where there are more tweets

//Get average price on days where there are more tweets than average
spark.sql("SELECT AVG(DailyAverage) FROM (SELECT Date, Sales, Tweets, DailyAverage FROM newMain WHERE Tweets > 15220.5673828125)").show()
// +-----------------+
// |avg(DailyAverage)|
// +-----------------+
// |2998.619289617485|
// +-----------------+

//Get average price on days where tweets are below average

scala> spark.sql("SELECT AVG(DailyAverage) FROM (SELECT Date, Sales, Tweets, DailyAverage FROM newMain WHERE Tweets < 15220.5673828125)").show()
// +-----------------+
// |avg(DailyAverage)|
// +-----------------+
// |389.4541261398176|
// +-----------------+
//Price is significantly higher on days where tweets are below average

//With this analysis we can at minimum conclude that tweets, sales, and price have a significant impact on each other. Generally When one is up, the others are all up. This shows at minimum that hype significantly contriubtes to the price(while it is also probable that reverse is somewhat true). It seems that the overall popularity of Etherum(on social media, in this case twitter), both amplifies price/sales numbers and is amplified by increases in price.

