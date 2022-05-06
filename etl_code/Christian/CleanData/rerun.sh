hdfs dfs -rm -r -f final/cleanSource
rm *.class rm *.jar

javac -classpath `yarn classpath` -d . CleanMapper.java
javac -classpath `yarn classpath` -d . CleanReducer.java
javac -classpath `yarn classpath`:. -d . Clean.java
jar -cvf Clean.jar *.class


hdfs dfs -mkdir final/cleanSource

hdfs dfs -mkdir final/cleanSource/input

hdfs dfs -put ethereum__eth.csv final/cleanSource/input
hdfs dfs -ls final/cleanSource/input
hadoop jar Clean.jar Clean final/cleanSource/input/ethereum__eth.csv /user/cbw307/final/cleanSource/output
hdfs dfs -cat final/cleanSource/output/part-r-00000