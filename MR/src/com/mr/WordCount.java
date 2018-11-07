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


public class WordCount {
    
    // chường trìng : mapper ==> combiner ==> reducer
    // map phrase
    // input của Map phrase <byteoffset(kiểu long), text>
    // ouput : <key : text, value : 1>
        public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        // hadoop cung cấp writable interface để serialize và 
        // de-serialization data được lưu trữ trong HDFS, và input và ouput giữa các mapreduce
        private Text word = new Text();
        private IntWritable one = new IntWritable(1);
        
        // LongWritable thực chất là keiu623 long là vi trí của dòng trong file
        // mapper context object nhận configuation, job, từ contructor của nó dùng để communicate giữa các map phrase
        // và giữa map phrase và reduce phrase
        @Override
        public void map(LongWritable key, Text value, Context output)
                throws IOException, InterruptedException{
            String line = value.toString();
            
            String []str = line.split("\\s+");
            
            for (int i = 0; i < str.length; ++i){
                word.set(str[i]);
                // write intermedia data between map phrase
                output.write(word, one);
            }
        }
    }
     
     // combiner phrase : lấy ouput của map phrase xử lý, ouput là key-value collection
     // reduce phrase
     // intput : key, values collection
     public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
         
         // context 
         @Override
         public void reduce (Text key, Iterable<IntWritable> collect, Context context) throws IOException, InterruptedException{
             int sum = 0; // số lần của key xuất hiện trong file
             for (IntWritable v : collect){
                 sum += v.get();
             }
             
             IntWritable result = new IntWritable(sum);
             
             context.write(key, result);
         }
     }
     
     
     public static void main(String []args)throws Exception{
         // job configuation
         
         Configuration conf = new Configuration();
         Job job = Job.getInstance(conf, "word count");
         // set class mà hadoop sẽ xác định file .jar nào được send tới các nodes để thực hiện
         // map task và reduce task (mapp class, reduce task phải là một phần của class này)
         job.setJarByClass(WordCount.class);
         
         job.setMapperClass(Map.class);
         job.setCombinerClass(Reduce.class);
         job.setReducerClass(Reduce.class);
         
         job.setInputFormatClass(TextInputFormat.class);
         
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(IntWritable.class);
         
         FileInputFormat.addInputPath(job, new Path(args[1]));
         FileOutputFormat.setOutputPath(job, new Path(args[2]));
         
            System.exit(job.waitForCompletion(true) ? 0 : 1);
     }
}
