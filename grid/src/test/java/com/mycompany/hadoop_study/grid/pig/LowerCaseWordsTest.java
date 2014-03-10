package com.mycompany.hadoop_study.grid.pig;


import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by tianzy on 3/5/14.
 */
public class LowerCaseWordsTest {
    private static String PIG_SCRIPT = "src/main/scripts/pig/LowerCaseWords.pig";
    private String myUDFJar = "target/hadoop_study-grid-jar-with-dependencies.jar";

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

    @BeforeClass
    public void findJar() throws IOException {
        String classPath = System.getProperty("java.class.path");
        System.out.println("java.class.path=" + classPath);

        for (String item : classPath.split(":")) {
            if (item.endsWith(".jar") && item.contains("hadoop_study-core")) {
                myUDFJar = item;
                System.out.println("using " + myUDFJar);
                break;
            }
        }
    }
    @Test
    public void test() throws IOException, ParseException {
        System.out.println("user.dir=" + System.getProperty("user.dir"));
        String[] args = {
                "Input=input_stub",
                "Output=output_stub",
                "MyUDF=" + myUDFJar,
        };

        String[] input  = {
                "THIS IS A SIMPLE TEST CASE",
                "THIS IS A SIMPLE TEST CASE",
        };
        String[] output = {
                gen(',', "this is a simple test case"),
                gen(',', "this is a simple test case"),
        };

        PigTest pigTest = new PigTest(PIG_SCRIPT, args);
        pigTest.assertOutput("LINE", input, "LOWER_LINE", output);
    }
}

