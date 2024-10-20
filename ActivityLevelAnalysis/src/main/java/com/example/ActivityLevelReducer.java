package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ActivityLevelReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Set<String> activeDates = new HashSet<>();
        for (Text val : values) {
            String[] parts = val.toString().split(",");
            String reportDate = parts[0];
            String isActive = parts[1];
            if (isActive.equals("1")) {
                activeDates.add(reportDate);
            }
        }
        int activeNum = activeDates.size();
        result.set(String.valueOf(activeNum));
        context.write(key, result);
    }
}