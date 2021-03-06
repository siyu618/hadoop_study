package com.mycompany.hadoop_study.grid.pig.eval;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.Util;
import org.testng.annotations.Test;

/**
 * Created by tianzy on 3/7/14.
 */
public class MyUPPERTest {
    private static EvalFunc<String> myUpper = new MyUPPER();
    @Test
    public void testExec() throws Exception {
        Tuple tuple = Util.buildTuple();
        assert null == myUpper.exec(tuple);

        tuple = Util.buildTuple("");
        assert "".equals(myUpper.exec(tuple));

        tuple = Util.buildTuple("123");
        assert "123".equals(myUpper.exec(tuple));

        tuple = Util.buildTuple("Jone");
        assert "JONE".equals(myUpper.exec(tuple));
    }
}
