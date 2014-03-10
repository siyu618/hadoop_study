package com.mycompany.hadoop_study.pig.eval;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.Util;
import org.testng.annotations.Test;

/**
 * Created by tianzy on 3/7/14.
 */
public class CommonLowerTest {
    private static EvalFunc<String> lower = new CommonLower();

    @Test
    public void testExec() throws Exception {
        Tuple tuple = Util.buildTuple();
        assert null == lower.exec(tuple);

        tuple = Util.buildTuple("");
        assert "".equals(lower.exec(tuple));

        tuple = Util.buildTuple("123");
        assert "123".equals(lower.exec(tuple));

        tuple = Util.buildTuple("JonE");
        assert "jone".equals(lower.exec(tuple));
    }
}
