package com.kapild.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;

import com.kapild.TextPair;
import com.kapild.WordCountMapper;

public class BaseMapperTest {
    private MapDriver<LongWritable, Text, TextPair, TextPair> mapDriver;

    @Before
    public void setUp() throws Exception {
        WordCountMapper classUnderTest = new WordCountMapper();
        mapDriver = new MapDriver<LongWritable, Text, TextPair, TextPair>(
                classUnderTest);
    }

    // @Test
    public void withOneWordCity() throws Exception {
        //
        // String factualId = "10003679";
        //
        // String cityName = "Francisco";
        //
        // String factualCityName = cityName;
        //
        // TextFloatPair prefixValue = new TextFloatPair();
        //
        // List<String> prefixList = new ArrayList<String>();
        //
        //
        // ArrayList<TextFloatPair> prefixes = new ArrayList<TextFloatPair>();
        // for (int i = 0; i < prefixList.size(); i++) {
        // prefixes.add(new TextFloatPair(prefixList.get(i), Float
        // .parseFloat(factualPopularity.toString())));
        // }
        //
        // // run mapper
        // mapDriver
        // .withInput(new Text(factualId), new Text(factualCityNameSepPop))
        // .withOutput(prefixes.get(0), prefixValue)
        // .withOutput(prefixes.get(1), prefixValue)
        // .withOutput(prefixes.get(2), prefixValue)
        // .withOutput(prefixes.get(3), prefixValue)
        // .withOutput(prefixes.get(4), prefixValue)
        // .withOutput(prefixes.get(5), prefixValue)
        // .withOutput(prefixes.get(6), prefixValue)
        // .withOutput(prefixes.get(7), prefixValue)
        // .withOutput(prefixes.get(8), prefixValue).runTest();
        //
    }

}
