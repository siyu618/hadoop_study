package com.mycompany.hadoop_study.grid.pig;

import org.apache.pig.ExecType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class PigTestBase {
	
    PigServer pigServer ;
    PigContext pigContext ;
    Cluster cluster ;
    
	public PigTestBase() {
		
		super();
	}

	@BeforeClass
	public void setup() throws IOException {
	     pigServer = new PigServer(ExecType.LOCAL);
	     pigContext = pigServer.getPigContext();
	     cluster = new Cluster(pigContext);
	}

    protected MockPigFeed mockPigFeed(PigTest test) {
        return new MockPigFeed(test, pigServer, cluster);
    }

    protected PigTest createDefaultTest(String[] args, String scriptPath) throws IOException {
        return new PigTest(scriptPath, args, pigServer, cluster);
    }

    protected PigTest createDefaultTest(String[] args, String[] argFiles, String scriptPath) throws IOException {
        List<String> list = new ArrayList<String>(Arrays.asList(args));
        list.addAll(readPropertiesFile(argFiles));
        String[] allArgs = new String[list.size()];
        list.toArray(allArgs);
        return new PigTest(scriptPath, allArgs, pigServer, cluster);
    }


    protected List<String> readPropertiesFile(String[] filePaths) throws IOException {
       Properties properties = new Properties();
        List<String> togo = new ArrayList<String>();

        for(String filePath:filePaths)
        {
                PropertiesUtil.loadPropertiesFromFile(properties, filePath);
        }
        for(String propertyName:properties.stringPropertyNames())
        {
            togo.add(propertyName + "=" + properties.getProperty(propertyName));
        }
        return  togo;
    }
}