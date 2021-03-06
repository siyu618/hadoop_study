package com.mycompany.hadoop_study.grid.hcat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatPartition;

import java.util.List;

/**
 * Created by tianzy on 4/16/14.
 */
public class GetPartitions2 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GetPartitions2(), args);
        if (0 != res) {
            throw new RuntimeException("GetPartitions2 failed...");
        }
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("GetPartitions <db_nme> <table_name> <partition_filter>");
            return 0;
        }
        String dbName = args[0];
        String tableName = args[1];
        String partitionFilter = args[2];
        System.out.println("dbName = [" + dbName + "]");
        System.out.println("tableName = [" + tableName + "]");
        System.out.println("partitionFilter = [" + partitionFilter + "]");

        Configuration conf = new Configuration();
        HCatClient hCatClient = null;
        try {
            hCatClient = HCatClient.create(conf);
            List<HCatPartition> hCatPartitionList = hCatClient.listPartitionsByFilter(dbName, tableName, partitionFilter);
            System.out.println("Found # of partitions : " + hCatPartitionList.size());
            for (HCatPartition hCatPartition : hCatPartitionList) {
                System.out.println(hCatPartition.getLocation());
            }
        } finally {
            if (null != hCatClient) {
                hCatClient.close();
            }
        }
        return 0;
    }
}
