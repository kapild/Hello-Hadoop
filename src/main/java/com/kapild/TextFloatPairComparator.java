package com.kapild;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextFloatPairComparator extends WritableComparator {

    protected TextFloatPairComparator() {
        super(TextFloatPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TextFloatPair pair1 = (TextFloatPair) a;
        TextFloatPair pair2 = (TextFloatPair) b;
        return pair1.getFirst().compareTo(pair2.getFirst());
    }
}
