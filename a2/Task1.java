import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task1 {

  public static class Mapper1
          extends Mapper<Object, Text, Text, Text>{

    private Text movie = new Text();
    private Text users = new Text();

    @Override
    protected void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

      String[] tokens = value.toString().split(",");
      movie.set(tokens[0]);

      int max = 0;
      StringBuilder sb = new StringBuilder();
      for (int i = 1; i < tokens.length; i++) {
        if (tokens[i].length() > 0) {
          int v = Integer.parseInt(tokens[i]);
          if (v > max) {
            sb.setLength(0);
            sb.append(i);
            max = v;
          } else if (v == max) {
            sb.append(",");
            sb.append(i);
          }
        }
      }

      users.set(sb.toString());
      context.write(movie, users);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }

    Job job = new Job(conf, "Task1");
    job.setJarByClass(Task1.class);
    job.setMapperClass(Task1.Mapper1.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
