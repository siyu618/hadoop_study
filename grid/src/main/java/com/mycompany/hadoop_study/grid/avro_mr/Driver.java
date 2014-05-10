package com.mycompany.hadoop_study.grid.avro_mr;


import com.mycompany.hadoop_study.avro.CampaignEvent;
import com.mycompany.hadoop_study.config.mapreduce.Config;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by tianzy on 5/5/14.
 */
public class Driver extends Configured implements Tool {
    Logger log = LogManager.getLogger(Driver.class);
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: Driver <inputDir> <outputDir> <lineTextFile> ");
            return -1;
        }
        String inputDir = args[0];
        String outputDir = args[1];
        //String lines = args[2];
        Path inputPath = new Path(inputDir);
        Path outputPath = new Path(outputDir);

        Configuration configuration = getConf();
        Config.setOozieDefaults(configuration);
        Config.trace(configuration);

        Job job = Job.getInstance(configuration);
        job.setJarByClass(Driver.class);
        job.setJobName("PT Split line records.");
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        if (outputPath.getFileSystem(configuration).exists(outputPath)) {
            outputPath.getFileSystem(configuration).delete(outputPath, true);
        }

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapperClass(SplitLineMapper.class);
        AvroJob.setInputKeySchema(job, CampaignEvent.SCHEMA$);
        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(NullWritable.class);
        AvroJob.setMapOutputKeySchema(job, CampaignEvent.SCHEMA$);

        job.setReducerClass(SplitLineReducer.class);
        // TODO: use LazyOutputFormat to remove the empty file under the baseOutputDir
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        AvroJob.setOutputKeySchema(job, CampaignEvent.SCHEMA$);

        job.getConfiguration().set("mapred.mapper.new-api", "true");
        job.getConfiguration().set("mapred.reducer.new-api", "true");
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        job.getConfiguration().set("mapreduce.user.classpath.first", "true");
        //should not use the org.apache.avro.mapred.AvroKeyComparator;
        job.setSortComparatorClass(org.apache.avro.hadoop.io.AvroKeyComparator.class);
        job.getConfiguration().set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization,org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization,org.apache.hadoop.io.serializer.avro.AvroReflectSerialization,org.apache.avro.hadoop.io.AvroSerialization");
        job.getConfiguration().set("avro.serialization.key.reader.schema", CampaignEvent.SCHEMA$.toString());
        job.getConfiguration().set("avro.serialization.key.writer.schema", CampaignEvent.SCHEMA$.toString());

        log.info("submit the job : " + job.getJobName());

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Driver(), args);
        if (0 != res) {
            throw new RuntimeException("Driver failed...");
        }
        System.exit(res);
    }
}
