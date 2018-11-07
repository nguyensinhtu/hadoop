/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mr;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.FloatWritable;
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
public class BT5_MaxTemp {
    
    
    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable>{
        
        @Override
        public void map(LongWritable key, Text value, Context output)
        
        throws IOException, InterruptedException{
            String line = value.toString();
            String fields[] = line.split("\\s+");
            if (fields.length <= 17)
                return;
            Text year = new Text(fields[2].substring(0, 4));
            Float temp = Float.parseFloat(fields[3]);
            
            output.write(year, new FloatWritable(temp));
        }
    }
    
    
    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable>{
        
        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context output) throws IOException, InterruptedException{
            float maxtemp = 0;
            for (FloatWritable v : values){
                float tmp = v.get();
                maxtemp = (tmp > maxtemp ? tmp : maxtemp);
            }
            
            output.write(key, new FloatWritable(maxtemp));
        }
    }
    
//    public static void main(String[] args) throws Exception{
//                         Configuration conf = new Configuration();
//         Job job = Job.getInstance(conf, "max temp each year");
//         // set class mà hadoop sẽ xác định file .jar nào được send tới các nodes để thực hiện
//         // map task và reduce task (mapp class, reduce task phải là một phần của class này)
//         
//         // do data weater từng ngày của từng năm nàm trong từng file khác nhau
//         // nên phải đọc hết tất cả các file trong thư mục weater
//         FileSystem fs = FileSystem.get(conf);
//         RemoteIterator<LocatedFileStatus> fileStatus = fs.listFiles(new Path(args[1]), true);
//         while (fileStatus.hasNext()){
//             LocatedFileStatus status = fileStatus.next();
//             job.addFileToClassPath(status.getPath());
//         }
//         
//         job.setJarByClass(BT5_MaxTemp.class);
//         
//         job.setMapperClass(Map.class);
//         job.setCombinerClass(Reduce.class);
//         job.setReducerClass(Reduce.class);
//         
//         job.setInputFormatClass(TextInputFormat.class);
//         
//         job.setOutputKeyClass(Text.class);
//         job.setOutputValueClass(FloatWritable.class);
//         
//         FileInputFormat.addInputPath(job, new Path(args[1]));
//         FileOutputFormat.setOutputPath(job, new Path(args[2]));
//         
//            System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
}
