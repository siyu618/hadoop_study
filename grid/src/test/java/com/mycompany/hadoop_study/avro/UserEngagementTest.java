package com.mycompany.hadoop_study.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created by tianzy on 4/28/14.
 */
public class UserEngagementTest {
    private static String BASE_PATH = "target/test/resources/user_engagement";

    void genTestData() throws IOException {
        // write
        UserEngagement userEngagement = UserEngagement.newBuilder().setBID("bid1").setSID("sid1").setDATE("20130403").setNETWORKADCLICKS(5L).setNETWORKIMPRESSIONS(100L).setMOBILENETWORKIMPRESSIONS(3L).setMOBILENETWORKADCLICKS(0L).build();
        UserEngagement userEngagement2 = UserEngagement.newBuilder().setBID("bid2").setSID("sid2").setDATE("20130403").setNETWORKADCLICKS(5L).setNETWORKIMPRESSIONS(100L).setMOBILENETWORKIMPRESSIONS(3L).setMOBILENETWORKADCLICKS(0L).build();

        FileUtils.forceMkdir(new File(BASE_PATH));
        System.out.println(BASE_PATH);
        File file = new File(BASE_PATH + "/engagement.avro");
        DatumWriter<UserEngagement> userDatumWriter = new SpecificDatumWriter<UserEngagement>(UserEngagement.class);
        DataFileWriter<UserEngagement> dataFileWriter = new DataFileWriter<UserEngagement>(userDatumWriter);
        dataFileWriter.create(userEngagement.getSchema(), file);
        dataFileWriter.append(userEngagement);
        dataFileWriter.append(userEngagement2);

        dataFileWriter.close();

        // read
        DatumReader<UserEngagement> datumReader = new SpecificDatumReader<>(UserEngagement.class);
        DataFileReader<UserEngagement> dataFileReader = new DataFileReader<>(file, datumReader);
        UserEngagement tmp;
        while (dataFileReader.hasNext()) {
            tmp = dataFileReader.next();
            System.out.println(tmp);
        }


    }

    public static void main(String[] args) throws IOException {
        UserEngagementTest userEngagementTest = new UserEngagementTest();
        userEngagementTest.genTestData();

    }

}
