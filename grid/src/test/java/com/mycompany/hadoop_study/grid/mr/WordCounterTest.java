package com.mycompany.hadoop_study.grid.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tianzy on 3/4/14.
 */
public class WordCounterTest {

    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordCounterMapper mapper = new WordCounterMapper();
        WordCounterReducer reducer = new WordCounterReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        Configuration configuration = mapDriver.getConfiguration();
        configuration.set("is.case.ignored", "true");
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("This is a simple statement...; it is dummy?"));
        mapDriver.addOutput(new Text("this"), new IntWritable(1));
        mapDriver.addOutput(new Text("is"), new IntWritable(1));
        mapDriver.addOutput(new Text("a"), new IntWritable(1));
        mapDriver.addOutput(new Text("simple"), new IntWritable(1));
        mapDriver.addOutput(new Text("statement"), new IntWritable(1));
        mapDriver.addOutput(new Text("it"), new IntWritable(1));
        mapDriver.addOutput(new Text("is"), new IntWritable(1));
        mapDriver.addOutput(new Text("dummy"), new IntWritable(1));

        mapDriver.runTest();
        assertEquals("Expected Counter.INPUT_RECORD_NUM is one", 1, mapDriver.getCounters().findCounter(WordCounterMapper.Counter.INPUT_RECORD_NUM).getValue());
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> list = new ArrayList<IntWritable>();
        list.add(new IntWritable(1));
        list.add(new IntWritable(5));
        reduceDriver.withInput(new Text("this"), list);
        reduceDriver.withOutput(new Text("this"), new IntWritable(6));
        reduceDriver.runTest();
        assertEquals("Expected Counter.UNIQUE_WORD_NUM is one", 1, reduceDriver.getCounters().findCounter(WordCounterReducer.Counter.UNIQUE_WORD_NUM).getValue());

    }

    @Test
    public void testMR() throws IOException {
        mapReduceDriver.setKeyOrderComparator(new Text.Comparator());
        mapReduceDriver.getConfiguration().set("mapreduce.job.reduces", "1");
        mapReduceDriver.getConfiguration().set("is.case.ignored", "true");
        mapReduceDriver.withInput(new LongWritable(), new Text("This this this this this?"));
        mapReduceDriver.addOutput(new Text("this"), new IntWritable(5));
        mapReduceDriver.runTest();

    }
}
