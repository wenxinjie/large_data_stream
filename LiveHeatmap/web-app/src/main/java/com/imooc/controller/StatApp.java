package com.imooc.controller;


import com.imooc.domain.ResultBean;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.util.*;

@RestController
public class StatApp {


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

    @Test
    public Map<String, Integer> query() throws IOException {
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


        final List<ResultBean> beanList = null;
        for (Result result2 : resultScanner) {
            for (KeyValue rowKV : result2.raw()) {
                String key = new String(rowKV.getRow());
                String[] splits = key.split("/");
                String word = splits[0];
                System.out.println("Timestamp: " + rowKV.getTimestamp() + " ");

                Integer count = map.get(word);
                if (count == null) {
                    count = 0;
                }
                count++;
                map.put(word, count);
            }
        }

        return map;

//        Set<String> s = map.keySet();
//        Iterator it = s.iterator();
//        while (it.hasNext()) {
//            String key = (String) it.next();
//            Integer value = map.get(key);
//            System.out.println(key + ":" + value);
//            ResultBean bean = new ResultBean();
//
//            String[] splits = key.split(",");
//            double log = Double.parseDouble(splits[0]);
//            double lat = Double.parseDouble(splits[1]);
//            bean.setLng(log);
//            bean.setLat(lat);
//            bean.setCount(value);
//
//        }
//        return beanList;

    }



    @Autowired
//    ResultBeanService resultBeanService;
//    ResultFromHbase resultFromHbase;


//    @RequestMapping(value = "/get_data.js", method = RequestMethod.GET)
//    public ModelAndView get_data() {
//        return new ModelAndView("get_data");
//    }
//
//    @RequestMapping(value = "/map_sent", method = RequestMethod.GET)
//    public ModelAndView map() {
//
//        ModelAndView view = new ModelAndView("map_sent");
//
//        List<ResultBean> results = resultBeanService.query(2);
//
//        JSONArray jsonArray = JSONArray.fromObject(results);
//        view.addObject("data_points", jsonArray);
//
//        return view;
//    }

    @RequestMapping(value = "/maps", method = RequestMethod.GET)
    public ModelAndView maps() throws IOException {

        ModelAndView view = new ModelAndView("maps");


        JSONArray jsonArray = new JSONArray();

        List list=new JSONArray();


        before();

        Map<String, Integer> map1 = query();
        Set<String> s = map1.keySet();
        Iterator it = s.iterator();
        ResultBean bean = new ResultBean();
        while (it.hasNext()) {
            int i = 0;
            String key = (String) it.next();
            Integer value = map1.get(key);
            System.out.println(key + ":" + value);


            String[] splits = key.split(",");
            double log = Double.parseDouble(splits[0]);
            double lat = Double.parseDouble(splits[1]);
//            bean.setLng(log);
//            bean.setLat(lat);
//            bean.setCount(value);

            JSONObject jsonObject1 = new JSONObject();
            jsonObject1.put("lat", lat);
            jsonObject1.put("lng", log);
            jsonObject1.put("count", value);
            list.add(jsonObject1);
        }

        after();
//        JSONArray jsonArray = JSONArray.fromObject(bean);

        System.out.println(list);


        view.addObject("data_json", list);

        return view;
    }


//    @RequestMapping(value = "/map_stat", method = RequestMethod.GET)
//    public ModelAndView map_stat() {
//
//        ModelAndView view = new ModelAndView("mapstat");
//        List<ResultBean> results = resultBeanService.query(0);
//
//        JSONArray jsonArray = JSONArray.fromObject(results);
//
//        //System.out.println(jsonArray);
//
//
//        // 如何把我们从后台查询到的数据以json的方式返回给前台页面
//        view.addObject("data_json", jsonArray);
//
//        return view;
//    }

}
