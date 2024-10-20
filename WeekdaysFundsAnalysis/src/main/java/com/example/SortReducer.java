package com.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length != 3) {
                continue;
            }
            keyOut.set(parts[0]); 
            valueOut.set(parts[1] + "," + parts[2]); 
            context.write(keyOut, valueOut);
        }
    }
}
