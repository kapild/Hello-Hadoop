package com.kapild;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<TextPair, TextPair, Text, Text> {

    @Override
    public void reduce(TextPair key, Iterable<TextPair> tuples, Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        Iterator<TextPair> it = tuples.iterator();
        while (it.hasNext()) {
            it.next();
            sum = sum + 1;
        }
        context.write(key.getFirst(), new Text(String.valueOf(sum)));
    }
}
