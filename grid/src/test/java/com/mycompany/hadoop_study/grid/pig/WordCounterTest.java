package com.mycompany.hadoop_study.grid.pig;


import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by tianzy on 3/5/14.
 */
public class WordCounterTest {
    private static String PIG_SCRIPT = "grid/src/main/scripts/pig/WordCounter.pig";

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
    public void test() throws IOException, ParseException {
        System.out.println(System.getProperty("user.dir"));
        String[] args = {
                "Input=input_stub",
                "Output=output_stub",
        };

        String[] input  = {
                "this is a simple test case",
                "this is a simple test case",
        };
        String[] output = {
                gen(',', "aq", "2"),
                gen(',', "case", "2"),
                gen(',', "is", "2"),
                gen(',', "simple", "2"),
                gen(',', "test", "2"),
                gen(',', "this", "2"),
        };

        PigTest pigTest = new PigTest(PIG_SCRIPT, args);
        pigTest.assertOutput("RAW_LINE", input, "WORD_COUNT_A", output);
    }
}

