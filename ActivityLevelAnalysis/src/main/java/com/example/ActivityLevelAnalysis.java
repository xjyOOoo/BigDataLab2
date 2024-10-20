package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ActivityLevelAnalysis {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ActivityLevelAnalysisCombined <input path> <intermediate output path> <final output path>");
            System.exit(-1);
        }

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Activity Level Analysis - Step 1");
        job1.setJarByClass(ActivityLevelAnalysis.class);
        job1.setMapperClass(ActivityLevelMapper.class);
        job1.setReducerClass(ActivityLevelReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1])); 



        boolean success = job1.waitForCompletion(true);
        if (!success) {
            System.exit(1);
        }
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Activity Level Analysis - Step 2");
        job2.setJarByClass(ActivityLevelAnalysis.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setSortComparatorClass(DescendingDoubleComparator.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        success = job2.waitForCompletion(true);
        if (!success) {
            System.exit(1);
        }

        System.exit(0);
    }
}