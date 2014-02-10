package com.kapild;

import org.apache.hadoop.mapreduce.Partitioner;

public class TextFloatPairPartitioner extends
        Partitioner<TextFloatPair, TextFloatPair> {

    @Override
    public int getPartition(TextFloatPair joinKey, TextFloatPair value,
            int numOfPartitions) {
        return (joinKey.getFirst().hashCode() & Integer.MAX_VALUE)
                % numOfPartitions;
    }
}
