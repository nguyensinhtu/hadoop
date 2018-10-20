/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mr;

/**
 *
 * @author NGUYENSINHTU
 */

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordCount {
    
    // chường trìng : mapper ==> combiner ==> reducer
    // map phrase
    // input : text và số lượng text
     public class Map extends Mapper<Text, IntWritable, Text, IntWritable>
    {
        // hadoop cung cấp writable interface để serialize và 
        // de-serialization data được lưu trữ trong HDFS, và input và ouput giữa các mapreduce
        private Text word = new Text();
        private IntWritable one = new IntWritable(1);
        
        // LongWritable thực chất là keiu623 long là vi trí của position
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException{
            String line = value.toString();
            
            String []str = line.split(" ");
            
            for (int i = 0; i < str.length; ++i){
                word.set(str[i]);
                output.collect(word, one);
            }
        }
    }
     
     // reduce phrase
     public class Reduce extends Reducer<Object, Object, Object, Object>{
         
     }
}
