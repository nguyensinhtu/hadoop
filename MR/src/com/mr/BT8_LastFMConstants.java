/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author tuns
 */
public class BT8_LastFMConstants {

    // map
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context output) {
            String line = output.toString();

            String[] str = line.split("[|]");
            String userid = str[0];
            String trackid = str[1];
            try {
                output.write(new Text(trackid), new Text(userid));
            } catch (IOException | InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    // reduce
    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context output) {
            Set<String> uniqueId = new HashSet<>();
            for (Text userid : values) {
                uniqueId.add(userid.toString());
            }
            try {
                output.write(key, new IntWritable(uniqueId.size()));
            } catch (IOException | InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public static void main(String[] args)throws Exception {
                Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LastFM Constants");
        // set class mà hadoop sẽ xác định file .jar nào được send tới các nodes để thực hiện
        // map task và reduce task (mapp class, reduce task phải là một phần của class này)
        job.setJarByClass(BT8_LastFMConstants.class);
        job.setMapperClass(BT8_LastFMConstants.Map.class);
        job.setCombinerClass(BT8_LastFMConstants.Reduce.class);
        job.setReducerClass(BT8_LastFMConstants.Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
