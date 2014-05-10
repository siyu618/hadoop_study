package com.mycompany.hadoop_study.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created by tianzy on 4/28/14.
 */
public class CampaignEventTest {
    private static String BASE_PATH = "target/test/resources/user_engagement";

    void genTestData() throws IOException {
        CampaignEvent campaignEvent = CampaignEvent.newBuilder()
                .setSID("sid1").setBID("bid1").setIO("IO1").setDATE("20140403")
                .setPROPERTYBOOKING("property_booking1").setSITE("site1")
                .setPOSITION("pos1").setLINE("line1")
                .setDEVICE("device1").setOS("os1")
                .setBROWSER("browser1").setCARRIER("carrier")
                .setCREATIVEID("creative_id1")
                .setIMPRESSIONS(1L).setCLICKS(0L)
                .setSITEID("site_id1").setDEVICEID("device_id1")
                .setOSID("os_id1").setBROWSERID("browser_id1")
                .setCARRIERID("carrier_id1").build();

        CampaignEvent campaignEvent1 = CampaignEvent.newBuilder(campaignEvent).setSID("sid2").setBID("bid2").build();


        FileUtils.forceMkdir(new File(BASE_PATH));
        System.out.println(BASE_PATH);
        File file = new File(BASE_PATH + "/campaign_event.avro");
        DatumWriter<CampaignEvent> campaignEventDatumWriter = new SpecificDatumWriter<CampaignEvent>(CampaignEvent.class);
        DataFileWriter<CampaignEvent> dataFileWriter = new DataFileWriter<CampaignEvent>(campaignEventDatumWriter);
        dataFileWriter.create(campaignEvent.getSchema(), file);
        dataFileWriter.append(campaignEvent);
        dataFileWriter.append(campaignEvent1);

        dataFileWriter.close();


        Schema schema = CampaignEvent.SCHEMA$;
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);
        GenericRecord user = null;
        System.out.println("read the data");
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }

    public static void main(String[] args) throws IOException {
        CampaignEventTest campaignEventTest = new CampaignEventTest();
        campaignEventTest.genTestData();;
    }
}
