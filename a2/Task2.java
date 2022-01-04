import java.io.IOException;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task2 {

    public static class RatingCountMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private Text word = new Text("c");
        private IntWritable count = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

            String[] tokens = value.toString().split(",");
			int total = 0;
            for(int k = 1; k<tokens.length  ; ++k){
                        if(tokens[k].length()>0){
                            total++;
                    }
                }

			count.set(total);
			context.write(word,count);

        }

    }


	 public static class RatingCountCombiner  extends Reducer<Text, IntWritable, Text, IntWritable> {
   	 private IntWritable result = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }


    public static class RatingCountReducer extends Reducer<Text, IntWritable, LongWritable, NullWritable> {
    private LongWritable result = new LongWritable();
    private static final NullWritable empty = NullWritable.get();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
      long sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(result, empty);
    }
  }




    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Task2");
        job.setJarByClass(Task2.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // add code here

        job.setMapperClass(RatingCountMapper.class);
		job.setCombinerClass(RatingCountCombiner.class);
        job.setReducerClass(RatingCountReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

