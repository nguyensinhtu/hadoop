/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mr;

import Utils.CSV;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
public class BT6_AvgSalary {

    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text name = new Text();
        private FloatWritable salary = new FloatWritable();

        @Override
        public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
            String line = value.toString();
            
            // bỏ qua header của file csv
            if (key.get() <= 0L) {
                return;
            }
                        
            String []fields = CSV.split(line);
            for (String s : fields)
                System.out.println(s);
            try {
            name.set(fields[5]);
            salary.set(Float.parseFloat(fields[2]));
            output.write(name, salary);
            }catch(NumberFormatException e){
                System.out.println(e.toString());
            }
        }
    }

    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context output) throws IOException, InterruptedException {
            int num = 0;
            float salary = 0.0f;
            for (FloatWritable f : values) {
                salary += f.get();
                num++;
            }
            
            float avgSalary = salary / num;
            output.write(key, new FloatWritable(avgSalary));
        }
    }

//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "average salary");
//        // set class mà hadoop sẽ xác định file .jar nào được send tới các nodes để thực hiện
//        // map task và reduce task (mapp class, reduce task phải là một phần của class này)
//        job.setJarByClass(Patent.class);
//
//        job.setMapperClass(Map.class);
//        job.setCombinerClass(Reduce.class);
//        job.setReducerClass(Reduce.class);
//
//        job.setInputFormatClass(TextInputFormat.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(FloatWritable.class);
//
//        FileInputFormat.addInputPath(job, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
////        String s = "\"Aarhus, Pam J.\",F,70959.79,71316.72,0.00,POL,Department of Police,MSB Information Mgmt and Tech Division Records Management Section,Fulltime-Regular,Office Services Coordinator,,09/22/1986";
////        Pattern pattern = Pattern.compile("\\s*(\\\"[^\\\"]*\\\"|[^,]*)\\s*");
////        Matcher matcher = pattern.matcher(s);
////        while (matcher.find()) {
////            System.out.println(matcher.group(1));
////        }
//    }
}
