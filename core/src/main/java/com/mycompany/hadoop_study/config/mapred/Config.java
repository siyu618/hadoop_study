package com.mycompany.hadoop_study.config.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class Config {

    public enum Key {
        MapredInput           ("mapreduce.input.fileinputformat.inputdir"),
        MapredOutput          ("mapreduce.output.fileoutputformat.outputdir");

        private final String keyName;
        private Key(String keyName) {
            this.keyName = keyName;
        }

        @Override
        public String toString() {
            return keyName;
        }
    }

    public static String getValue(Configuration conf, Key key) throws InvalidJobConfException {
        return getValue(conf, key, null);
    }

    public static String getValue(Configuration conf, Key key, String defaultValue) throws InvalidJobConfException {
        String value = conf.get(key.toString(), defaultValue);
        if (value == null) {
            throw new InvalidJobConfException("missing '" + key + "' configuration property");
        }
        log.info(key + "=" + value);
        return value;
    }

    public static void setValue(Configuration conf, Key key, String value) {
        conf.set(key.toString(), value);
        log.info("set " + key + "=" + value);
    }

    public static JobConf setOozieDefaults(JobConf conf) throws FileNotFoundException {
        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        }
        String oozieConf = System.getProperty("oozie.action.conf.xml");
        if (oozieConf != null) {
            conf.addResource(new FileInputStream(oozieConf));
        }
        return conf;
    }

    public static JobConf setHadoopDefaults(JobConf conf/*, String jobName*/) {
//        conf.setJobName(jobName);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        return conf;
    }

    public static JobConf setProperties(JobConf conf, Properties props) {
        Set<String> propSet = props.stringPropertyNames();
        for (String key : propSet) {
            conf.set(key, props.getProperty(key));
        }
        return conf;
    }

    public static void trace(Configuration conf) {
        Iterator<Map.Entry<String, String>> it = conf.iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> e = it.next();
            log.info("trace: " + e.getKey() + "=" + e.getValue());
        }
    }

    private static Logger log = Logger.getLogger(Config.class);
}
