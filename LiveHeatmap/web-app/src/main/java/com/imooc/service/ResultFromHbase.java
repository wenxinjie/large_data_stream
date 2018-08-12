package com.imooc.service;
//import com.google.common.util.concurrent.Service;
import com.imooc.domain.ResultBean;
import java.util.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.lang.*;

public class ResultFromHbase {
    private static String TABLE_NAME = "wc25";
    private static String FAMILY_NAME = "cf";
    private static String ROW_KEY1 = "word";
    private static String COLUMN_word = "word";
    private static String COLUMN_count = "count";
    Configuration conf;
    HBaseAdmin hBaseAdmin;
    HTable hTable;

    @Before
    public void before() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.rootDir", "hdfs://hadoop000:8020/hbase");
        conf.set("hbase.zookeeper.quorum", "hadoop000:2181");
        hBaseAdmin = new HBaseAdmin(conf);
        hTable = new HTable(conf, TABLE_NAME);
    }

    @After
    public void after() throws IOException {
        hTable.close();
        hBaseAdmin.close();
    }

    public List<ResultBean> scan() throws IOException {
        Scan scan = new Scan();
        System.out.println(System.currentTimeMillis());
//        System.out.println(System.currentTimeMillis()-5100000);
//        System.out.println(System.currentTimeMillis()-4940000);

        System.out.println(System.currentTimeMillis() - 30000);
        System.out.println(System.currentTimeMillis() + 10000);
//        scan.setTimeRange(System.currentTimeMillis() - 30000, System.currentTimeMillis() + 10000);

        scan.addColumn(FAMILY_NAME.getBytes(), COLUMN_count.getBytes());
        ResultScanner resultScanner = hTable.getScanner(scan);
        Map<String, Integer> map = new HashMap<String, Integer>();
        List<ResultBean> beanList = null;

        for (Result result2 : resultScanner) {
            for (KeyValue rowKV : result2.raw()) {
                String key = new String(rowKV.getRow());
                String[] splits = key.split("/");
                String word = splits[0];
                System.out.print("Timestamp: " + rowKV.getTimestamp() + " ");

                Integer count = map.get(word);
                if (count == null) {
                    count = 0;
                }
                count++;
                map.put(word, count);
            }
        }

        Set<String> s = map.keySet();
        Iterator it = s.iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            Integer value = map.get(key);
            System.out.println(key + ":" + value);
            ResultBean bean = new ResultBean();

            String[] splits = key.split(",");
            double log = Double.parseDouble(splits[0]);
            double lat = Double.parseDouble(splits[1]);
            bean.setLng(log);
            bean.setLat(lat);
            bean.setCount(value);
            beanList.add(bean);
        }
        return beanList;
        }
    }




