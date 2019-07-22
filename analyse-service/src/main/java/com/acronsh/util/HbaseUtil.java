package com.acronsh.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Administrator on 2018/10/29 0029.
 */
public class HbaseUtil {
    private static Admin admin = null;
    private static Connection conn = null;

    static {
        String host = "10.211.55.12";
        // 创建hbase配置对象
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://" + host + ":9000/hbase");
        //使用eclipse时必须添加这个，否则无法定位
        conf.set("hbase.zookeeper.quorum", host);
        conf.set("hbase.client.scanner.timeout.period", "600000");
        conf.set("hbase.rpc.timeout", "600000");
        try {
            conn = ConnectionFactory.createConnection(conf);
            // 得到管理程序
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 创建表
     */
    public void createTable(String tabName, String famliyname) throws Exception {
        HTableDescriptor tab = new HTableDescriptor(tabName);
        // 添加列族,每个表至少有一个列族.
        HColumnDescriptor colDesc = new HColumnDescriptor(famliyname);
        tab.addFamily(colDesc);
        // 创建表
        admin.createTable(tab);
        System.out.println("over");
    }

    /**
     * 插入数据，create "baseuserscaninfo","time"
     */
    public static void put(String tablename, String rowkey, String famliyname, Map<String, String> datamap) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tablename));
        // 将字符串转换成byte[]
        byte[] rowkeybyte = Bytes.toBytes(rowkey);
        Put put = new Put(rowkeybyte);
        if (datamap != null) {
            Set<Map.Entry<String, String>> set = datamap.entrySet();
            for (Map.Entry<String, String> entry : set) {
                String key = entry.getKey();
                Object value = entry.getValue();
                put.addColumn(Bytes.toBytes(famliyname), Bytes.toBytes(key), Bytes.toBytes(value + ""));
            }
        }
        table.put(put);
        table.close();
        System.out.println("ok");
    }

    /**
     * 获取数据，create "baseuserscaninfo","time"
     * create "pindaoinfo","info"
     * create "userinfo","info"
     * create "productinfo","info"
     * create "orderinfo","info"
     */
    public static String getdata(String tablename, String rowkey, String famliyname, String column) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tablename));
        // 将字符串转换成byte[]
        byte[] rowkeybyte = Bytes.toBytes(rowkey);
        Get get = new Get(rowkeybyte);
        Result result = table.get(get);
        byte[] resultbytes = result.getValue(famliyname.getBytes(), column.getBytes());
        if (resultbytes == null) {
            return null;
        }

        return new String(resultbytes);
    }

    /**
     * 插入数据，create "baseuserscaninfo","time"
     */
    public static void putdata(String tablename, String rowkey, String famliyname, String column, String data) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tablename));
        Put put = new Put(rowkey.getBytes());
        put.addColumn(famliyname.getBytes(), column.getBytes(), data.getBytes());
        table.put(put);
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/Users/wangyakun/coding/hadoop");
        String string = getdata("baseuserscaninfo", "1", "time", "fisrtvisittime");
        System.out.println(string);
    }
}

