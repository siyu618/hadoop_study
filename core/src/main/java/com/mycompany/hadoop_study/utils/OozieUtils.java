package com.mycompany.hadoop_study.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Created by tianzy on 4/21/14.
 */
public class OozieUtils {
    public static final String OOZIE_ACTION_OUTPUT_PROPERTIES = "oozie.action.output.properties";
    private Logger log = LogManager.getLogger(OozieUtils.class);

    public static void writePropertiesToOozie(Map<String, String> configuration) throws IOException {
        String oozieProp = System.getProperty(OOZIE_ACTION_OUTPUT_PROPERTIES);
        if (oozieProp != null) {
            File propFile = new File(oozieProp);
            Properties props = new Properties();
            if (null != configuration) {
                for (String key : configuration.keySet()) {
                    props.setProperty(key, configuration.get(key));
                }
            }
            OutputStream os = new FileOutputStream(propFile);
            props.store(os, "");
            os.close();
        } else {
            throw new RuntimeException(OOZIE_ACTION_OUTPUT_PROPERTIES + " System property not defined");
        }
    }
}
