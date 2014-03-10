package com.mycompany.hadoop_study.config.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.util.*;

public class Config {

	private static final Config instance = new Config();
	public static Config getInstance() { return instance; }

    public static Configuration setOozieDefaults(Configuration conf) throws FileNotFoundException {
        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        }
        String oozieConf = System.getProperty("oozie.action.conf.xml");
        if (oozieConf != null) {
            conf.addResource(new Path("file:///",
                    System.getProperty("oozie.action.conf.xml")));
        }
        return conf;
    }


    public static Configuration setProperties(Configuration conf, Properties props) {
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
