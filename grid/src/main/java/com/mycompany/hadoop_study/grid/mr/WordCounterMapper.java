package com.mycompany.hadoop_study.grid.mr;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by tianzy on 3/4/14.
 */
public class WordCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private IntWritable One = new IntWritable(1);
    private Boolean isCaseIgnored = false;

    enum Counter {
        INPUT_RECORD_NUM;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        isCaseIgnored = context.getConfiguration().getBoolean("is.case.ignored", Boolean.FALSE);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = StringUtils.split(line, ",.;? \t");
        // add the check for mock-it test
        // for MR unit, we don't need this.
        if (null != context.getCounter(Counter.INPUT_RECORD_NUM)) {
            context.getCounter(Counter.INPUT_RECORD_NUM).increment(1);
        }
        for (String word : words) {
            if (isCaseIgnored) {
                word = word.toLowerCase();
            }
            // System.out.println("word[" + word + "]");
            context.write(new Text(word), One);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
