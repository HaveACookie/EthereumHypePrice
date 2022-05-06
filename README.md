<h1>Ether Price Analysis</h1>

<p>
Inside this folder are a few subdirectories labeled as direction. /ana_code contains analytic data and code. Any scala file can be run in the spark shell. The python file should be run in pyspark. In etl_code there are mapreduce projects that can be run using the included shell scripts. Profiling code is located in /profiling_code, and can be run using the spark shell. Screenshots are included in the /screenshots folder. Any further directions are included in individual readme files.
</p>

<h1>Running The Code</h1>

<p>To run the code, first download the data from the data ingestion folder and then put the files on hdfs. following this run map reduce jobs from the etl code directory. Following this, take those results and run the profiling code. And finally analytics can run on the analytic folder.</p>