hdfs dfs -rm -r -f final/recCount
rm *.class rm *.jar

javac -classpath `yarn classpath` -d . CountRecsMapper.java
javac -classpath `yarn classpath` -d . CountRecsReducer.java
javac -classpath `yarn classpath`:. -d . CountRecs.java
jar -cvf CountRecs.jar *.class


hdfs dfs -mkdir final/recCount

hdfs dfs -mkdir final/recCount/input

hdfs dfs -put ethereum__eth.csv final/recCount/input
hdfs dfs -ls final/recCount/input
hadoop jar CountRecs.jar CountRecs final/recCount/input/ethereum__eth.csv /user/cbw307/final/recCount/output
hdfs dfs -cat final/recCount/output/part-r-00000