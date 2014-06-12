//package com.mycompany.hadoop_study.utils;
//
//
//import com.mycompany.hadoop_study.config.mapred.Config;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.*;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.permission.FsPermission;
//import org.apache.hadoop.io.compress.CompressionCodec;
//import org.apache.hadoop.io.compress.CompressionCodecFactory;
//import org.apache.hadoop.io.compress.CompressionOutputStream;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//import org.apache.log4j.Logger;
//
//import java.io.*;
//import java.util.Properties;
//
//@Deprecated
//public class CopyTextToFile extends Configured implements Tool {
//
//	private Logger log = Logger.getLogger(CopyTextToFile.class);
//
//	@Override
//	public int run(String[] args) throws Exception {
//        JobConf jconf = new JobConf(getConf(), CopyTextToFile.class);
//        Config.setHadoopDefaults(jconf);
//        Config.setOozieDefaults(jconf);
//        Properties properties = null;
//		Config.setProperties(jconf, props);
//
//		Config.trace(jconf);
//
//		String srcFiles = Config.getValue(jconf, Config.Key.SrcFiles);
//		String dstFile = Config.getValue(jconf, Config.Key.DstFile);
//		String doneFile = Config.getValue(jconf, Config.Key.DstDoneFile);
//
//		FileSystem fs = FileSystem.get(jconf);
//		FileStatus[] srcFilesStat = fs.globStatus(new Path(srcFiles));srcFiles
//		if (srcFilesStat == null) {
//			log.info("No files found to copy");
//			return 0;
//		}
//		log.info("found " + srcFilesStat.length + " files to copy");
//
//		CompressionCodecFactory codecFactory = new CompressionCodecFactory(jconf);
//
//		Path dstPath = new Path(dstFile);
//		FSDataOutputStream ofs = fs.create(dstPath);
//		//TODO to find out why permissions are not set by default
//		fs.setPermission(dstPath, FsPermission.getDefault().applyUMask(FsPermission.getUMask(jconf)));
//		CompressionCodec oCodec = codecFactory.getCodec(dstPath);
//		CompressionOutputStream os = oCodec.createOutputStream(ofs);
//		log.info("destination file " + dstFile + " is created");
//
//		for (FileStatus fst : srcFilesStat) {
//			if (fst.isDir()) {
//				log.info("skip directory " + fst.getPath().toString());
//				continue;
//			}
//			if (fst.getLen() == 0) {
//				log.info("skip empty file " + fst.getPath().toString());
//				continue;
//			}
//
//			CompressionCodec iCodec = codecFactory.getCodec(fst.getPath());
//			FSDataInputStream ifs = fs.open(fst.getPath());
//			InputStream is = (iCodec != null) ? iCodec.createInputStream(ifs) : ifs;
//
//			transferData(is, os);
//			is.close();
//			log.info("file " + fst.getPath().toString() + " is read");
//		}
//		os.close();
//
//		log.info("file " + dstFile + " is saved");
//
//		if (!doneFile.isEmpty()) {
//			Path donePath = new Path(doneFile);
//			FSDataOutputStream donefs = fs.create(donePath);
//			donefs.close();
//			//TODO to find out why permissions are not set by default
//			fs.setPermission(donePath, FsPermission.getDefault().applyUMask(FsPermission.getUMask(jconf)));
//
//			log.info("done file " + doneFile + " is created");
//		}
//
//		log.info("done");
//		return 0;
//	}
//
//	private void transferData(InputStream is, OutputStream os) throws IOException {
//		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
//		Writer writer = new OutputStreamWriter(os);
//		String line;
//		while ((line = reader.readLine()) != null) {
//			writer.write(line);
//			writer.write("\n");
//		}
//		writer.flush();
//	}
//
//	public static void main(String[] args) throws Exception {
//		int rc = ToolRunner.run(new Configuration(), new CopyTextToFile(), args);
//		System.exit(rc);
//	}
//}
