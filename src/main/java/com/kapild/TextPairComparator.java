package com.kapild;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextPairComparator extends WritableComparator {

    protected TextPairComparator() {
        super(TextPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TextPair pair1 = (TextPair) a;
        TextPair pair2 = (TextPair) b;
        return pair1.getFirst().compareTo(pair2.getFirst());
    }
}
