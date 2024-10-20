package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date; 
import java.util.Locale; 

public class WeekdaysFundsMapper extends Mapper<Object, Text, Text, Text> {
    private Text weekdayKey = new Text();
    private Text inflowOutflow = new Text();
    private boolean isFirstLine = true;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line == null || line.isEmpty()) {
            return;
        }
        if (isFirstLine && line.contains("report_date")) {
            isFirstLine = false;
            return;
        }

        String[] fields = line.split(",");
        if (fields.length < 9) {
            System.err.println("Invalid input line (not enough fields): " + line);
            return;
        }

        try {
            String totalPurchaseAmt = fields[4].trim();
            String totalRedeemAmt = fields[8].trim();
            if (totalPurchaseAmt.isEmpty()) {
                totalPurchaseAmt = "0";
            }
            if (totalRedeemAmt.isEmpty()) {
                totalRedeemAmt = "0";
            }

            Date reportDate = dateFormat.parse(fields[1].trim());
            SimpleDateFormat dayFormat = new SimpleDateFormat("EEEE", Locale.ENGLISH);
            String weekday = dayFormat.format(reportDate);
            String dateStr = dateFormat.format(reportDate);
            weekdayKey.set(weekday);
            inflowOutflow.set(dateStr + "," + totalPurchaseAmt + "," + totalRedeemAmt);

            context.write(weekdayKey, inflowOutflow);
        } catch (ParseException e) {
            System.err.println("ParseException in line: " + line);
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.err.println("NumberFormatException in line: " + line);
            e.printStackTrace();
        }
    }
}