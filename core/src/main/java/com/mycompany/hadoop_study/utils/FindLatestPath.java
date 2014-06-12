package com.mycompany.hadoop_study.utils;

import com.yahoo.partner_targeting.user_match.Config;
import com.yahoo.partner_targeting.utils.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class FindLatestPath extends Configured implements Tool {

	private static Logger log = Logger.getLogger(FindLatestPath.class);

	public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new Configuration(), new FindLatestPath(), args);
        System.exit(rc);
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf jconf = new JobConf(getConf(), FindLatestPath.class);
		Config.setHadoopDefaults(jconf);
		Config.setOozieDefaults(jconf);
		
		Properties props = new Properties();
		setOozieOutputProperties(jconf, props);

		File oozieOutputProps = new File(System.getProperty("oozie.action.output.properties"));
		log.info("save properties to " + oozieOutputProps.getAbsolutePath());
		
		OutputStream os = new FileOutputStream(oozieOutputProps);
		props.store(os, null);
		os.close();
		
		log.info("done");
		return 0;
	}
	
	private void setOozieOutputProperties(JobConf jconf, Properties props) throws IOException {
		String toProcess = Config.getValue(jconf, Config.Key.FindLatestPaths);
		for (String pair : StringUtils.parseCSV(toProcess)) {
			String[] keyVal = pair.split("=");
			if (keyVal.length != 2) {
				log.error("skip invalid setting " + pair);
				continue;
			}
			Path path = new Path(keyVal[1]);
			FileSystem fsys = path.getFileSystem(jconf);
			FileStatus[] fstats = fsys.listStatus(path);

			FileStatus last = null;
			if (fstats != null) {
				for (FileStatus fstat : fstats) {
					if (last == null) {
						last = fstat;
						continue;
					}
					if (fstat.getPath().getName().compareTo(last.getPath().getName()) > 0) {
						last = fstat;
					}
				}
			}
			if (last == null) {
				log.warn("can't find a file for " + pair);
				continue;
			}

			props.put(keyVal[0], last.getPath().toString());
			log.info(keyVal[0] + " <= " + last.getPath().toString());
		} 
	}
}
