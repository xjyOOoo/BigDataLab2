package com.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, DoubleWritable, Text> {
    private DoubleWritable activeNumKey = new DoubleWritable();
    private Text userIdValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\\t");
        if (parts.length != 2) {
            System.err.println("Invalid input line: " + line);
            return;
        }

        String userId = parts[0];
        String activeNumStr = parts[1];
        double activeNum = 0.0;
        try {
            activeNum = Double.parseDouble(activeNumStr);
        } catch (NumberFormatException e) {
            System.err.println("Invalid number: " + activeNumStr);
            return;
        }

        activeNumKey.set(activeNum);
        userIdValue.set(userId);
        context.write(activeNumKey, userIdValue);
    }
}