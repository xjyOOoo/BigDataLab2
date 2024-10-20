package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WeekdaysFundsReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        
        double totalInflow = 0.0;
        double totalOutflow = 0.0;
        Set<String> uniqueDates = new HashSet<>();
        Map<String, Double[]> dateToAmounts = new HashMap<>();

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length < 3) {
                System.err.println("Invalid value (not enough parts): " + val.toString());
                continue;
            }
            try {
                String date = parts[0];
                double inflow = Double.parseDouble(parts[1]);
                double outflow = Double.parseDouble(parts[2]);

                uniqueDates.add(date);

                if (dateToAmounts.containsKey(date)) {
                    Double[] amounts = dateToAmounts.get(date);
                    amounts[0] += inflow;
                    amounts[1] += outflow;
                } else {
                    dateToAmounts.put(date, new Double[]{inflow, outflow});
                }
            } catch (NumberFormatException e) {
                System.err.println("NumberFormatException in value: " + val.toString());
                e.printStackTrace();
                continue;
            }
        }

        int dateCount = uniqueDates.size();
        if (dateCount == 0) {
            System.err.println("No valid dates for key: " + key.toString());
            return;
        }

        for (Double[] amounts : dateToAmounts.values()) {
            totalInflow += amounts[0];
            totalOutflow += amounts[1];
        }

        double avgInflow = totalInflow / dateCount;
        double avgOutflow = totalOutflow / dateCount;
        result.set(avgInflow + "," + avgOutflow);
        context.write(key, result);
    }
}