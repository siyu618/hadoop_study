package com.mycompany.hadoop_study.grid.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by tianzy on 3/4/14.
 */
public class WordCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    enum Counter {
        UNIQUE_WORD_NUM;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        for (IntWritable intWritable : values) {
            total += intWritable.get();
        }
        context.write(key, new IntWritable(total));
        context.getCounter(Counter.UNIQUE_WORD_NUM).increment(1);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
