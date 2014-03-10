package com.mycompany.hadoop_study.grid.pig.eval;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Created by tianzy on 3/7/14.
 */
public class MyUPPER extends EvalFunc<String> {
    @Override
    public String exec(Tuple tuple) throws IOException {
        if (null == tuple || tuple.size() == 0) {
        return null; }
        String str = (String) tuple.get(0);
        return  str.toUpperCase();
    }
}
