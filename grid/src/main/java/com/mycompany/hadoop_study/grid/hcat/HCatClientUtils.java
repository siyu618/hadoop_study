package com.mycompany.hadoop_study.grid.hcat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatPartition;
import org.apache.hcatalog.common.HCatException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Created by tianzy on 4/16/14.
 */
public class HCatClientUtils {
    private static Logger log = LogManager.getLogger(HCatClientUtils.class);
    private static HCatClient hCatClient = null;

    public static HCatClient initHCatClient() throws HCatException {
        if (null == hCatClient) {
            hCatClient = HCatClient.create(new Configuration());
        }
        return hCatClient;
    }

    public static int getPartitionNumber(String dbName, String tableName, String partitionFilter) throws HCatException {
        HCatClient hCatClient = initHCatClient();
        List<HCatPartition> hCatPartitionList = hCatClient.listPartitionsByFilter(dbName, tableName, partitionFilter);
        if (hCatPartitionList != null) {
            return hCatPartitionList.size();
        } else {
            return 0;
        }
    }
}
