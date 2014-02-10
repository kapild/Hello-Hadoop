package com.kapild;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextFloatPair implements WritableComparable<TextFloatPair> {

    public static final String LOWER_KEY = "0";
    public static final String HIGHER_KEY = "1";

    public static final Text LOWER_KEY_TEXT = new Text(LOWER_KEY);
    public static final Text HIGHER_KEY_TEXT = new Text(HIGHER_KEY);

    public static final IntWritable LOWER_KEY_INT = new IntWritable(0);
    public static final IntWritable HIGHER_KEY_INT = new IntWritable(1);

    private final Text first;
    private final FloatWritable second;

    public Text getFirst() {
        return first;
    }

    public FloatWritable getSecond() {
        return second;
    }

    public TextFloatPair() {
        first = new Text();
        second = new FloatWritable();
    }

    public TextFloatPair(String first, float second) {
        this(new Text(first), new FloatWritable(second));
    }

    public TextFloatPair(Text first, FloatWritable second) {
        this.first = first;
        this.second = second;
    }

    public void set(TextFloatPair textPair) {
        set(textPair.getFirst(), textPair.getSecond());
    }

    public void set(Text first, FloatWritable second) {
        this.first.set(first);
        this.second.set(second.get());
    }

    public void set(String first, float second) {
        this.first.set(first);
        this.second.set(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    /**
     * The compare returns the -ive of float value. This is required as we are
     * sorting the object in descending order.
     */
    @Override
    public int compareTo(TextFloatPair that) {
        int cmp = first.compareTo(that.first);
        if (cmp == 0) {
            cmp = -second.compareTo(that.second);
        }
        return cmp;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj instanceof TextFloatPair) {
            TextFloatPair that = (TextFloatPair) obj;
            return (first.equals(that.first) && second.equals(that.second));
        }

        return false;
    }

    @Override
    public int hashCode() {
        return first.hashCode() + 167 * second.hashCode();
    }

    @Override
    public String toString() {
        return first.toString() + "-" + second.toString();
    }
}