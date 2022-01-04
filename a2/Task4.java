import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {

  public static class Mapper4
          extends Mapper<Object, Text, Text, IntWritable>{

    private static Map<String, byte[]> map = new HashMap<>();
    private Text name = new Text();
    private IntWritable similarity = new IntWritable();

    private void readFile(File f) {
      try {
        BufferedReader reader = new BufferedReader(new FileReader(f));
        String line = reader.readLine();
        while (line != null) {
          if (line.length() > 0) {
            String[] tokens = line.split(",");
            byte[] list = new byte[tokens.length - 1];
            for (int i = 1; i < tokens.length; i++) {
              if (tokens[i].length() > 0) {
                list[i - 1] = Byte.parseByte(tokens[i]);
              } else {
                list[i - 1] = 0;
              }
            }
            map.put(tokens[0], list);
          }
          line = reader.readLine();
        }
      } catch (IOException e) {
        System.err.println("IOException when reading files from local file: " + f.getPath());
      }
    }

    @Override
    protected void setup(Context context) {

      File f = new File("./inputData");
      if (f.isDirectory()) {
        File[] files = f.listFiles();
        if (files != null) {
          for (File fi : files) {
            readFile(fi);
          }
        }
      } else if (f.isFile()) {
        readFile(f);
      }
    }

    @Override
    protected void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

      String [] tokens = value.toString().split(",", 2);
      String movieName1 = tokens[0];
      byte[] rates1 = map.get(movieName1);

      for (Map.Entry<String, byte[]> entry : map.entrySet()) {
        String movieName2 = entry.getKey();
        byte[] rates2 = entry.getValue();
        if (movieName1.compareTo(movieName2) < 0) {
          name.set(movieName1 + "," + movieName2);

          int num = Math.min(rates2.length, rates1.length);
          int count = 0;
          for (int i = 0; i < num; i++) {
            if (rates1[i] == rates2[i] && rates1[i] != 0) {
              count++;
            }
          }
          similarity.set(count);
          context.write(name, similarity);
        }
      }
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

    Job job = new Job(conf, "Task4");
    job.setJarByClass(Task4.class);
    job.setMapperClass(Task4.Mapper4.class);
    job.setNumReduceTasks(0);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.addCacheFile(new URI(otherArgs[0] + "#inputData"));

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
