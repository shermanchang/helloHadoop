package com.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;


/*** Page 57 JVM marked***/
public class MyJob extends Configured implements Tool{
    /*** Why use Class Configured and Interface Tool***/

    public static class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
        public  void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            /*** Reporter: report progress(float) and update counters(Iterable, Enum class), status information etc ***/
            output.collect(value, key);
            /*** collect is a generics to save a key/value, you can convert it to list/set/map,
             * i.e collect().toMap();
             * hadoop is main for Map and Reduce
             ***/

        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String csv = "";
            while (values.hasNext()) {
                if (csv.length() > 0) csv += ",";
                csv += values.next().toString();
            }

            output.collect(key, new Text(csv));
        }
    }

    @Override
    /*** run is a driver for the mapreduce***/
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        /*** conf is a general configuration for the hadoop***/

        JobConf job = new JobConf(conf, MyJob.class);
        /*** job created in the memory, we need use set method to re-define some specific parameters for the job  ***/

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job,in);
        FileOutputFormat.setOutputPath(job,out);

        job.setJobName("MyJob");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        /**
         * KeyValueTextInputFormat default separator is tab
         */
        job.set("key.value.separator.in.input.line", ",");

        JobClient.runJob(job);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MyJob(), args);
        System.exit(res);
    }
}