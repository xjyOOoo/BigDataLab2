package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class DailyFundsReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sumInflow = 0.0;
        double sumOutflow = 0.0;
        for (Text val : values) {
            String[] amounts = val.toString().split(",");
            try {
                double inflow = Double.parseDouble(amounts[0]);
                double outflow = Double.parseDouble(amounts[1]);
                sumInflow += inflow;
                sumOutflow += outflow;
            } catch (NumberFormatException e) {
                continue;
            }
        }
        result.set(sumInflow + "," + sumOutflow);
        context.write(key, result);
    }
}