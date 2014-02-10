package com.kapild;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
        Mapper<LongWritable, Text, TextPair, TextPair>

{

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String tokens[] = value.toString().split("\\s+");

        context.write(new TextPair(), new TextPair());

    }

}
