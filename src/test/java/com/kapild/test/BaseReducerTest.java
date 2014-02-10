package com.kapild.test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.kapild.TextPair;
import com.kapild.WordCountReducer;

public class BaseReducerTest {

    private ReduceDriver<TextPair, TextPair, Text, Text> reduceDriver;

    @Before
    public void setUp() throws Exception {
        WordCountReducer classUnderTest = new WordCountReducer();
        reduceDriver = new ReduceDriver<TextPair, TextPair, Text, Text>(
                classUnderTest);
    }

    @Test
    public void testReduceBothValues() throws Exception {

        // String prefix = "san";
        //
        // String factualId1 = "10003679";
        // String cityName1 = "San Francisco";
        // float po1 = 10f;
        //
        // String factualId2 = "10003671";
        // String cityName2 = "New York";
        // float po2 = 8f;
        //
        // String factualId3 = "10003271";
        // String cityName3 = "Austin";
        // float po3 = 7f;
        //
        // float popularity = 10f;
        //
        // TextFloatPair key = new TextFloatPair(prefix, popularity);
        //
        // TextFloatPair pairValue1 = new TextFloatPair(
        // FactualCityPrefixMapper.getPrefixValue(factualId1, cityName1,
        // po1), new FloatWritable(po1));
        //
        // TextFloatPair pairValue2 = new TextFloatPair(
        // FactualCityPrefixMapper.getPrefixValue(factualId2, cityName2,
        // po2), new FloatWritable(po2));
        //
        // TextFloatPair pairValue3 = new TextFloatPair(
        // FactualCityPrefixMapper.getPrefixValue(factualId3, cityName3,
        // po3), new FloatWritable(po3));
        //
        // List<TextFloatPair> testRows = new ArrayList<TextFloatPair>();
        // testRows.add(pairValue1);
        // testRows.add(pairValue2);
        // testRows.add(pairValue3);
        //
        // reduceDriver.withInput(key, testRows)
        // .withOutput(new Text(key.getFirst()), new Text(outText))
        // .runTest();
        //
        // // // check the counters
        // Collection<String> groupNames = reduceDriver.getCounters()
        // .getGroupNames();
        // assertThat(groupNames.size(), is(1));
        // CounterGroup counterGroup = reduceDriver.getCounters().getGroup(
        // groupNames.iterator().next());
        // for (Counter counter : counterGroup) {
        // if (counter
        // .getName()
        // .equalsIgnoreCase(
        // WordCountReducer.WordCountReducerCounter.numOfSuggestions
        // .name())) {
        // assertThat(counter.getValue(), is(3L));
        // }
        // }
    }
}
