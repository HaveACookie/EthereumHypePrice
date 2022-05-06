import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{
          //split line by comma
        if(value.toString().startsWith("id")){
            return;
        }
        String[] line = value.toString().split(",", -1);
        //This filters out rows that dont have tweet counts and rows that have null values
        //This also removes other columns from the dataset as this data source has many rows that frankly dont matter for my purposes
        String date = line[2];
        String tweets = line[11];
        //This Filters
        if(tweets.length()<1 || date.equals("1970/01/01") || tweets.equals("null")){
            return;
        }
        else{
//            context.write(new Text("wrong length"), new IntWritable(num));
//            context.write(new Text(line), new IntWritable(num));
            context.write(new Text(date), new IntWritable(Integer.parseInt(tweets)));
            return;
        }
            
        }
}

