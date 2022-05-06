hdfs dfs -rm -r -f final/cleanCount
rm *.class rm *.jar

javac -classpath `yarn classpath` -d . CountRecsMapper.java
javac -classpath `yarn classpath` -d . CountRecsReducer.java
javac -classpath `yarn classpath`:. -d . CountRecs.java
jar -cvf CountRecs.jar *.class

hdfs dfs -get final/cleanSource/output/part-r-00000

hdfs dfs -mkdir final/cleanCount

hdfs dfs -mkdir final/cleanCount/input

hdfs dfs -put part-r-00000 final/cleanCount/input
hdfs dfs -ls final/cleanCount/input
hadoop jar CountRecs.jar CountRecs final/cleanCount/input/part-r-00000 /user/cbw307/final/cleanCount/output
hdfs dfs -cat final/cleanCount/output/part-r-00000


