/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mr;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
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
public class WeatherData {

    public static class MapWeather extends Mapper<LongWritable, Text, Text, Text> {

        private Text date = new Text();
        private Text label = new Text();

        @Override
        public  void map(LongWritable key, Text value, Context ouput) throws IOException, InterruptedException {
            String s = value.toString();
            String str[] = s.split("\\s+");
            if (str.length <= 17)
                return;
            date.set(str[2]);
            String num = str[17].replaceAll("\\*", "");
            float MaxTemp = Float.parseFloat(num);
            num = str[18].replaceAll("\\*", "");
            float MinTemp = Float.parseFloat(num);

            label.set("unknown");
            if (MinTemp < 10.0f) {
                label.set("Cold Day");
            } else if (MaxTemp > 40.0f) {
                label.set("Hot Day");
            }
            
            ouput.write(date, label);
        }
    }

//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//
//        Job job = Job.getInstance(conf, "maximum temperature");
//
//        job.setJarByClass(WeatherData.class);
//        job.setMapperClass(MapWeather.class);
//        job.setNumReduceTasks(0);
//
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }

}
