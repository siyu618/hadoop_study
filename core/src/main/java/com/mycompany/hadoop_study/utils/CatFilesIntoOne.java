package com.mycompany.hadoop_study.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Created by tianzy on 2/19/14.
 */
public class CatFilesIntoOne extends Configured implements Tool {
    private static Logger log = Logger.getLogger(CatFilesIntoOne.class);


    private List<Path> pathsToCat = new LinkedList<Path>();
    private Path destinationPath = null;
    private Boolean overwrite = Boolean.TRUE;
    private static Configuration configuration = new Configuration();

    public static void showUsage() {
        System.out.println("Usage   : java CatFilesToOneFile <srcFile1,...> <destFile> [overwrite]");
        System.out.println("Example : java CatFilesToOneFile hdfs:/path/to/files/* hdfs:/path/to/file/file_name true");
        System.out.println("        : [overwrite] default is true. ");
    }

    public static OutputStream streamToStream(InputStream in, OutputStream out)
            throws IOException {
        byte[] buf = new byte[2048];
        for (int ret = in.read(buf); ret != -1; ret = in.read(buf)) {
            if (ret > 0) {
                out.write(buf, 0, ret);
            }
        }
        return out;
    }

    public static List<Path> getPaths(String srcPath) throws IOException {
        List<Path> pathList = new ArrayList<Path>();
        if (srcPath.trim().isEmpty()) {
            return pathList;
        }
        Path path = new Path(srcPath.trim());
        FileSystem fs = path.getFileSystem(configuration);
        FileStatus[] srcFileStats = fs.globStatus(path);
        if (null == srcFileStats) {
            log.info("Skip input item: [" + srcPath + "] none matched files");
            return pathList;
        }
        for (FileStatus srcFileStat : srcFileStats) {
            if (srcFileStat.isDirectory()) {
                log.info("Skip directory : " + srcFileStat.getPath().toString());
                continue;
            }
            if (srcFileStat.getPath().getName().startsWith(".")) {
                log.info("Skip hidden file : " + srcFileStat.getPath().toString());
                continue;
            }
            if (srcFileStat.getLen() == 0) {
                log.info("Skip zero size file : " + srcFileStat.getPath().toString());
                continue;
            }
            pathList.add(srcFileStat.getPath());
        }
        return pathList;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            showUsage();
            return 1;
        }
        String srcPaths = args[0];
        String destPathString = args[1];
        if (args.length > 2) {
            if (Boolean.FALSE.toString().equalsIgnoreCase(args[2])) {
                overwrite = Boolean.FALSE;
            }
        }

        Path destPath = new Path(destPathString);
        if (destPath.getFileSystem(configuration).exists(destPath)) {
            log.info("Dest path [" + destPathString + "] exists.");
        }

        int totalFiles = 0;
        FSDataOutputStream outStream = null;
        try {
            log.info("create dest Path: " + destPath.toString());
            outStream = destPath.getFileSystem(configuration).create(destPath, overwrite);
            for (String srcPath : StringUtils.split(srcPaths, ",")) {
                log.info("===start===");
                log.info("Processing input item: [" + srcPath + "]");
                List<Path> pathList = getPaths(srcPath);
                totalFiles += pathList.size();
                log.info("srcPath[" + srcPath + "] matches " + pathList.size() + " files.");
                for (Path path : pathList) {
                    log.info("merging file : " + path.toString());
                    FSDataInputStream inStream = null;
                    try {
                        inStream = path.getFileSystem(configuration).open(path);
                        IOUtils.copyBytes(inStream, outStream, configuration, false);
                    } finally {
                        IOUtils.closeStream(inStream);
                    }
                }
            }
        } finally {
            IOUtils.closeStream(outStream);
        }
        log.info("  # of merged files : " + totalFiles);
        writeOozie(totalFiles);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CatFilesIntoOne(), args);
        if (0 != res) {
            throw new RuntimeException("job failed, check the related job please ....");
        }
    }

    private static void writeOozie(int fileCount) throws IOException {
        // Passing the variable to WF that could be referred by subsequent
        // actions
        File file = new File(
                System.getProperty("oozie.action.output.properties"));
        Properties props = new Properties();
        props.setProperty("MergedFileNumber", fileCount + "");
        OutputStream os = new FileOutputStream(file);
        props.store(os, "");
        os.close();
    }
}
