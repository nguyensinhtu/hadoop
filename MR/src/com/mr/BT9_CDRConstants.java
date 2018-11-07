/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;
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
public class BT9_CDRConstants {

    public static SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static Long calculateMins(String s, String t) {
        Date sd, ed = null;
        Long mins = 0L;
        try {
            sd = dateformat.parse(s);
            ed = dateformat.parse(t);
//            Calendar c1 = Calendar.getInstance();
//            Calendar c2 = Calendar.getInstance();
//            c1.setTime(sd);
//            c2.setTime(ed);
             mins = TimeUnit.MINUTES.convert(ed.getTime() - sd.getTime(), TimeUnit.MILLISECONDS);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
        }
        
        return mins;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        public void map(LongWritable key, Text value, Context output) {

            String line = value.toString();
            String[] str = line.split("\\|");
            String phoneNumber = str[0];
            String startTime = str[2];
            String endTime = str[3];
            int flag = Integer.parseInt(str[4]);
            Long mins = calculateMins(startTime, endTime);
            if (flag == 1) {
                try {
                    output.write(new Text(phoneNumber), new LongWritable(mins));
                } catch (IOException | InterruptedException e) {
                    e.getMessage();
                }
            }
        }
    }

    // reduce phrase
    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context output) {
            Long sumMins = 0L;
            for (LongWritable min : values) {
                sumMins += min.get();
            }
//            try {
//                output.write(key, new LongWritable(sumMins));
//            } catch (IOException | InterruptedException e) {
//                System.out.println(e.getMessage());
//            }
            if (sumMins >= 60) {
                try {
                    output.write(key, new LongWritable(sumMins));
                } catch (IOException | InterruptedException e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

//    public static void main(String[] args) throws Exception {
//
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "call data record program");
//        // set class mà hadoop sẽ xác định file .jar nào được send tới các nodes để thực hiện
//        // map task và reduce task (mapp class, reduce task phải là một phần của class này)
//        job.setJarByClass(BT9_CDRConstants.class);
//        job.setMapperClass(BT9_CDRConstants.Map.class);
//        job.setCombinerClass(BT9_CDRConstants.Reduce.class);
//        job.setReducerClass(BT9_CDRConstants.Reduce.class);
////        job.setNumReduceTasks(0);
//        job.setInputFormatClass(TextInputFormat.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(LongWritable.class);
//
//        FileInputFormat.addInputPath(job, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//
//    }
}
