package com.mycompany.hadoop_study.grid.avro_mr;

import com.google.gson.Gson;

import com.mycompany.hadoop_study.avro.CampaignEvent;
import com.mycompany.hadoop_study.utils.GsonFactory;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Created by tianzy on 4/22/14.
 */
public class SplitLineReducer extends Reducer<AvroKey<CampaignEvent>, NullWritable, AvroKey<CampaignEvent>, NullWritable> {
    private static Logger log = LogManager.getLogger(SplitLineReducer.class);
    private AvroMultipleOutputs amos = null;
    private static Gson gson = GsonFactory.getNonPrettyGson();

    @Override
    protected void reduce(AvroKey<CampaignEvent> key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String line = (String)key.datum().getLINE();
        String io = (String)key.datum().getIO();
        String outputPath = genOutputPath(line, io);
        System.out.println("line:[" + line + "],io:[" + io + "],outputPath:[" + outputPath + "]");
        for (NullWritable value : values) {
            if (!StringUtils.isEmpty(outputPath)) {
                amos.write(key, NullWritable.get(), outputPath);
            } else {
                log.error("Drop bad record whose line or io is empty " + gson.toJson(key.datum()));
            }
        }
    }


    public String genOutputPath(String line, String io) {
        if (StringUtils.isEmpty(line) || StringUtils.isEmpty(io)) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(io).append("/").append(line).append("/").append("part");
        return sb.toString();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        amos = new AvroMultipleOutputs(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        amos.close();
        super.cleanup(context);
    }
}
