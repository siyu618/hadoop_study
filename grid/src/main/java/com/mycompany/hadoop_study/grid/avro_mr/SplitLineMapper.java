package com.mycompany.hadoop_study.grid.avro_mr;

import com.mycompany.hadoop_study.avro.CampaignEvent;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by tianzy on 4/22/14.
 */
public class SplitLineMapper extends Mapper<
        AvroKey<CampaignEvent>, NullWritable,
        AvroKey<CampaignEvent>, NullWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(AvroKey<CampaignEvent> key, NullWritable value, Context context) throws IOException, InterruptedException {
        System.out.println("mapper:" + key.datum());
        context.write(key, NullWritable.get());
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
