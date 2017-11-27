package com.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class CitationHistogram extends Configured implements Tool {


    /**
     * MapClass returns <citation_count,list(UNO)> for the Reducer
     */
    public static class MapClass extends MapReduceBase implements Mapper<Text, Text, IntWritable, IntWritable> {
        private final static IntWritable UNO = new IntWritable(1);
        private IntWritable citationCount = new IntWritable();

        public void map(Text key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
            citationCount.set(Integer.parseInt(value.toString()));
            output.collect(citationCount, UNO);
        }
    }

    /**
     *** It is only a Combine example, no logic for CitationHistogram Class:
     * public static class Combine extends MapReduce implements Reducer<Text, Text, Text, Text> {
     *     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) {
     *         double sum = 0;
     *         int count = 0;
     *         while (values.hasNext()) {
     *             String fields[] = values.next().toString().split(*,*);
     *             sum += Double.parseDouble(fields[0]);
     *             count += Integer.parseInt(fields[1]);
     *         }
     *         output.collect(key, new Text(sum + "," + count));
     *     }
     * }
     * */

    /**
     * Reduce output.collect() collect the <sitation_count, list(UNO)>
     * and do a sum for the list(UNO)
     * returns <sitation_count, sum>
     */
    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, Reporter report) throws IOException {
            int count = 0;
            while (values.hasNext()) {
                count += values.next().get();
            }
            output.collect(key, new IntWritable(count));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        JobConf job = new JobConf(conf, CitationHistogram.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job,in);
        FileOutputFormat.setOutputPath(job,out);

        job.setJobName("CitationHistogram");
        job.setMapperClass(MapClass.class);
        /*** Add Combine.class into driver, JobConf point to Combine.class ***/
//        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        JobClient.runJob(job);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CitationHistogram(), args);
        System.exit(res);
    }
}
