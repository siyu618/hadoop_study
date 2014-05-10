package com.mycompany.hadoop_study.grid.pig;

import com.mycompany.hadoop_study.avro.CampaignEvent;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by tianzy on 4/29/14.
 */
public class CampaignStatsTest {
    private static String BASE_PATH = "target/test_data/user_engagement";
    private static String PigScript = "src/main/scripts/pig/CampaignStats.pig";
    private String campaignData = BASE_PATH + "/" + "campaign.avro";
    private String campaignInfo = BASE_PATH + "/" + "lineInfo.txt";
    private static String PIG_BANK = "";

    @BeforeClass
    public void beforeClass() throws IOException {
        FileUtils.forceMkdir(new File(BASE_PATH));

       // AvroStorage

        String classPath = System.getProperty("java.class.path");
        System.out.println("java.class.path=" + classPath);

        for (String item : classPath.split(":")) {
            if (item.endsWith(".jar") && item.contains("piggybank")) {
                PIG_BANK = item;
                System.out.println("using " + PIG_BANK);
                break;
            }
        }
    }

    @AfterClass
    public void afterClass() throws IOException {
        FileUtils.forceDelete(new File(BASE_PATH));
    }

    private String gen(char del, String... fields) {
        String line = null;
        for (String f : fields) {
            if (line != null) {
                line += del;
            } else {
                line = "";
            }
            line += f;
        }
        if (del == ',') {
            line = "(" + line + ")";
        }
        return line;
    }

    // CHD does not have the piggybank.jar
    //@Test
    public void test() throws IOException, ParseException {
        // prepare data

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
        CampaignEvent campaignEvent1 = CampaignEvent.newBuilder(campaignEvent)
                .setLINE("line1").setIMPRESSIONS(100L).setCLICKS(1L).build();
        CampaignEvent campaignEvent2 = CampaignEvent.newBuilder(campaignEvent)
                .setLINE("line1").setIMPRESSIONS(20L).setCLICKS(1L).build();
        CampaignEvent campaignEvent3 = CampaignEvent.newBuilder(campaignEvent)
                .setLINE("line2").setIMPRESSIONS(10L).setCLICKS(1L).build();
        CampaignEvent campaignEvent4 = CampaignEvent.newBuilder(campaignEvent)
                .setLINE("line3").setIMPRESSIONS(1L).setCLICKS(0L).build();

        File file = new File(campaignData);
        DatumWriter<CampaignEvent> campaignEventDatumWriter = new SpecificDatumWriter<CampaignEvent>(CampaignEvent.class);
        DataFileWriter<CampaignEvent> dataFileWriter = new DataFileWriter<CampaignEvent>(campaignEventDatumWriter);
        dataFileWriter.create(campaignEvent.getSchema(), file);
        dataFileWriter.append(campaignEvent1);
        dataFileWriter.append(campaignEvent2);
        dataFileWriter.append(campaignEvent3);
        dataFileWriter.append(campaignEvent4);
        dataFileWriter.close();

        File lineInfo = new File(campaignInfo);
        // format : line|io|date|adsystem
        String lineInfos[] =  {
                gen('|', "line1", "io1", "20130403", "apt_adw"),
                gen('|', "line2", "io2", "20130403", "apt_adw"),
                gen('|', "line5", "io5", "20130403", "apt_adw"),
        };
        FileUtils.writeStringToFile(lineInfo, StringUtils.join(lineInfos, "\n"));

        // expected output
        String[] expectOutput = {
                gen(',', "line1", "apt_adw", "120", "2"),
                gen(',', "line2", "apt_adw", "10", "1"),
                gen(',', "line5", "apt_adw", "0", "0"),
        };

        String[] args = {
                "campaign_event=" + campaignData,
                "line_io_info=" + campaignInfo,
                "PIG_BANK=" + PIG_BANK,
                "output=output_stub",
        };

        System.out.println(StringUtils.join(args, "\n"));

        PigTest pigTest = new PigTest(PigScript, args);
        pigTest.assertOutput("line_io_stats", expectOutput);
    }
}
