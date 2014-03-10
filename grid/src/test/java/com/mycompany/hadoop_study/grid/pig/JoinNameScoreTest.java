package com.mycompany.hadoop_study.grid.pig;


import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by tianzy on 3/5/14.
 */
public class JoinNameScoreTest extends PigTestBase {
    private static String PIG_SCRIPT = "src/main/scripts/pig/JoinNameScore.pig";

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

    @Test
    public void test() throws Exception {
        System.out.println(System.getProperty("user.dir"));
        String[] args = {
                "InputName=input_name_stub",
                "InputScore=input_score_stub",
                "Output=output_stub",
        };

        String[] inputName  = {
                gen('\t', "001", "zhangsan"),
                gen('\t', "002", "lisi"),
                gen('\t', "003", "wangwu"),
                gen('\t', "007", "Bound"),
        };
        String[] inputScore  = {
                gen('\t', "001", "90"),
                gen('\t', "002", "100"),
                gen('\t', "007", "92"),
                gen('\t', "009", "93"),
        };
        String[] output = {
                gen(',', "001", "zhangsan", "90"),
                gen(',', "007", "Bound", "92"),
                gen(',', "002", "lisi", "100"),
        };

        PigTest pigTest = super.createDefaultTest(args, PIG_SCRIPT);
        MockPigFeed mockPigFeed = mockPigFeed(pigTest);
        mockPigFeed.mockInputAlias("ID_NAME", inputName);
        mockPigFeed.mockInputAlias("ID_SCORE", inputScore);
        pigTest.assertOutput("ID_NAME_SCORE_ORDERED", output);
        mockPigFeed.cleanup();
    }
}

