package com.mycompany.hadoop_study.grid.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * Created by tianzy on 3/5/14.
 */
public class WordCounterMapperTest {
    @Test
    public void testMap() throws Exception {
        WordCounterMapper mapper = new WordCounterMapper();
        Mapper.Context context = mock(Mapper.Context.class);
        mapper.map(new LongWritable(), new Text("This is a simple statement...; it is dummy?"), context);
        verify(context).write(new Text("This"), new IntWritable(1));
        verify(context, times(2)).write(new Text("is"), new IntWritable(1));
        verify(context).write(new Text("a"), new IntWritable(1));
        verify(context).write(new Text("simple"), new IntWritable(1));
        verify(context).write(new Text("statement"), new IntWritable(1));
        verify(context).write(new Text("it"), new IntWritable(1));
        //verify(context).write(new Text("is"), new IntWritable(1));
        verify(context).write(new Text("dummy"), new IntWritable(1));
    }
}
