/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mr;

/**
 *
 * @author tuns
 */

import java.io.IOException;
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

public class Patent {
    
    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
        private IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context out) throws IOException, InterruptedException{
            String line = value.toString();
            String[] str = line.split("\t");
            out.write(new IntWritable(Integer.parseInt(str[0])), one);
        }
    }
    
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context output) throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable i : values){
                sum += i.get();
            }
            
            output.write(key, new IntWritable(sum));
        }
    }
    
//    public static void main(String[] args) throws Exception{
//                 Configuration conf = new Configuration();
//         Job job = Job.getInstance(conf, "patent program");
//         // set class mà hadoop sẽ xác định file .jar nào được send tới các nodes để thực hiện
//         // map task và reduce task (mapp class, reduce task phải là một phần của class này)
//         job.setJarByClass(Patent.class);
//         
//         job.setMapperClass(Map.class);
//         job.setCombinerClass(Reduce.class);
//         job.setReducerClass(Reduce.class);
//         
//         job.setInputFormatClass(TextInputFormat.class);
//         
//         job.setOutputKeyClass(IntWritable.class);
//         job.setOutputValueClass(IntWritable.class);
//         
//         FileInputFormat.addInputPath(job, new Path(args[1]));
//         FileOutputFormat.setOutputPath(job, new Path(args[2]));
//         
//            System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
}
