package com.kapild;

import org.apache.hadoop.mapreduce.Partitioner;

public class TextPairPartitioner extends Partitioner<TextPair, TextPair> {

    @Override
    public int getPartition(TextPair joinKey, TextPair value,
            int numOfPartitions) {
        return (joinKey.getFirst().hashCode() & Integer.MAX_VALUE)
                % numOfPartitions;
    }
}
