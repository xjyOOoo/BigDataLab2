package com.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, DoubleWritable, Text> {
    private DoubleWritable sortKey = new DoubleWritable();
    private Text weekdayValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\\t");
        if (parts.length != 2) {
            System.err.println("Invalid input line: " + line);
            return;
        }

        String weekday = parts[0];
        String[] amounts = parts[1].split(",");
        if (amounts.length != 2) {
            System.err.println("Invalid amounts: " + parts[1]);
            return;
        }

        try {
            double avgInflow = Double.parseDouble(amounts[0]);
            sortKey.set(avgInflow);
            weekdayValue.set(weekday + "," + amounts[0] + "," + amounts[1]);
            context.write(sortKey, weekdayValue);
        } catch (NumberFormatException e) {
            System.err.println("NumberFormatException in line: " + line);
        }
    }
}