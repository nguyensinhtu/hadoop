/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mr;

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

/**
 *
 * @author tuns
 */
public class WordSizeWordCount {
    
     
    // mapper tính wordsize
    // input : <byteofee, text>
    // ouput : <length của token, token>
    public static class MapWordSize extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
        private IntWritable one = new IntWritable(1);
        //private IntWritable len = new IntWritable();
        
        @Override
        public void map(LongWritable key, Text value, Context ouput) throws IOException, InterruptedException{
            String line = value.toString();
            String []tokens = line.split("\\s+|\\.");
            for (int i = 0; i < tokens.length; ++i){
                if (tokens[i].length() <= 0)
                    continue;
                //len.set(tokens[i].length());
                ouput.write(new IntWritable(tokens[i].length()), one);
            }
            
        }
    }
    
    
    // reducer
    // input : <len, list các word có cùng len>
    // ouput <len, số lượng word có cungw length>
        public static class ReduceWordSize extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context output) throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable i : values)
                sum += i.get();
            output.write(key, new IntWritable(sum));
        }
    }
    
//    public static void main(String[] args) throws Exception{
//        Configuration conf = new Configuration();
//        
//        Job job = Job.getInstance(conf, "word count word size");
//        
//        job.setJarByClass(WordSizeWordCount.class);
//        
//        job.setMapperClass(MapWordSize.class);
//        job.setCombinerClass(ReduceWordSize.class);
//        job.setReducerClass(ReduceWordSize.class);
//        
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputKeyClass(IntWritable.class);
//        job.setOutputValueClass(IntWritable.class);
//        
//        FileInputFormat.addInputPath(job, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));
//        
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
}
