package com.example;

import org.apache.hadoop.io.DoubleWritable;
public class DescendingDoubleComparator extends DoubleWritable.Comparator {
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return -super.compare(b1, s1, l1, b2, s2, l2);
    }
    @Override
    public int compare(Object a, Object b) {
        return -super.compare(a, b);
    }
}