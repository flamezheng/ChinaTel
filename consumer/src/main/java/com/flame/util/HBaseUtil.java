package com.flame.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;


public class HBaseUtil {
    private static Configuration conf = HBaseConfiguration.create();

    public static boolean isTableExists(String tableName) throws IOException {
        //获取连接
        Connection connection = ConnectionFactory.createConnection(conf);
        // 获取admin
        Admin admin = connection.getAdmin();

        //返回表是否存在
        boolean result = admin.tableExists(TableName.valueOf(tableName));

        //关闭相关资源
        close(connection, admin);
        return result;
    }

    //初始化命名空间
    public static void initNameSpace(String nameSpace) throws IOException {
        // 获取连接
        Connection connection = ConnectionFactory.createConnection(conf);
        // 获取admin对象
        Admin admin = connection.getAdmin();
        //创建namespace描述器
        NamespaceDescriptor descriptor = NamespaceDescriptor.create(nameSpace).build();
        //创建namespace
        admin.createNamespace(descriptor);
        //关闭相关资源
        close(connection, admin);
    }


    //创建表
    public static void createTable(String tableName, int regions, String... columnFamily) throws IOException {
        if (isTableExists(tableName)) {
            System.out.println("表" + tableName + "已存在");
            return;
        }
        //获取连接
        Connection connection = ConnectionFactory.createConnection(conf);
        //获取admin对象
        Admin admin = connection.getAdmin();
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //添加列族
        for (String cf : columnFamily) {
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        }
        //添加协处理器
        hTableDescriptor.addCoprocessor("com.flame.coprocessor.CalleeWriteObserver");
        //创建表（带分区键）
        admin.createTable(hTableDescriptor, getSplitKeys(regions));
        close(connection,admin);
    }

    /**
     * 预分区键
     *
     * @param regions
     * @return
     */
    private static byte[][] getSplitKeys(int regions) {
        DecimalFormat df = new DecimalFormat("00");
        byte[][] splitKeys = new byte[regions][];
        for (int i = 0; i < regions; i++) {
            splitKeys[i] = Bytes.toBytes(df.format(i) + "|");
        }

        for (byte[] splitKey : splitKeys) {
            System.out.println(Bytes.toString(splitKey));
        }
        return splitKeys;
    }

    /**
     * 生成rowkey
     * <p>
     * xxx_13612020000_20190802 13:21:11_13892928483_0180
     *
     * @param regionHash
     * @param caller
     * @param buildTime
     * @param callee
     * @param flag
     * @param duration
     * @return
     */
    public static String genRowKey(String regionHash, String caller, String buildTime,
                                   String callee, String flag, String duration) {
        return regionHash + "_"
                + caller + "_"
                + buildTime + "_"
                + callee + "_"
                + flag + "_"
                + duration;
    }

    /**
     * 生成分区号
     * 01_ 02_
     *
     * @param caller
     * @param buildTime
     * @param regions
     * @return
     */
    public static String getRegionHash(String caller, String buildTime, int regions) {
        int len = caller.length();
        //获取手机号后四位
        String last4Num = caller.substring(len - 4);
        //获取年月
        String yearMonth = buildTime.replaceAll("-", "").substring(0, 6);
        int regionCode = (Integer.valueOf(last4Num) ^ Integer.valueOf(yearMonth)) % regions;
        DecimalFormat df = new DecimalFormat("00");
        return df.format(regionCode);
    }


    private static void close(Connection connection, Admin admin, Table... tables) {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (tables.length <= 0) return;
        for (Table table : tables) {
            if (table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
