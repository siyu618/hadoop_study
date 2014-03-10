package com.mycompany.hadoop_study.grid.pig;


import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by tianzy on 3/5/14.
 */
public class CapitalizeWordsTest {
    private static String PIG_SCRIPT = "src/main/scripts/pig/CapitalizeWords.pig";
    private String myUDFJar = "";

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
                "MyUDF=target/hadoop_study-grid-jar-with-dependencies.jar"
        };

        String[] input  = {
                "this is a simple test case",
                "this is a simple test case",
        };
        String[] output = {
                gen(',', "THIS IS A SIMPLE TEST CASE"),
                gen(',', "THIS IS A SIMPLE TEST CASE"),
        };

        PigTest pigTest = new PigTest(PIG_SCRIPT, args);
        pigTest.assertOutput("LINE", input, "UPPER_LINE", output);
    }
}

