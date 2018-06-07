package com.flame.dao;

import com.flame.util.ConnectionInstance;
import com.flame.util.HBaseUtil;
import com.flame.util.PropertyUtil;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class HBaseDAO {

    private String nameSpace; //HBase命名空间
    private String tableName; //HBase表
    private int regions;   //HBase分区数
    private String cf;        //HBase列族

    private SimpleDateFormat sdf = null;
    private HTable table;
    private String flag;

    //缓存put对象的集合
    private List<Put> putList;

    public HBaseDAO() throws IOException {
        nameSpace = PropertyUtil.properties.getProperty("hbase.namespace");
        tableName = PropertyUtil.properties.getProperty("hbase.table.name");
        regions = Integer.parseInt(PropertyUtil.properties.getProperty("hbase.regions"));
        cf = PropertyUtil.properties.getProperty("hbase.table.cf");
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        putList = new ArrayList<Put>();
        flag = "1";
        if (!HBaseUtil.isTableExists(tableName)) {
            HBaseUtil.initNameSpace(nameSpace);
            HBaseUtil.createTable(tableName, regions, cf, "f2");
        }
    }

    public void put(String ori) throws IOException, ParseException {
        //创建连接及获取表
        if (putList.size() == 0) {
            Connection connection = ConnectionInstance.getInstance();
            table = (HTable) connection.getTable(TableName.valueOf(tableName));
            table.setAutoFlushTo(false);
            table.setWriteBufferSize(1024 * 1024);
        }
        //如果传输的数据为空直接返回
        if (ori == null) return;

        //ori:14314302040,19460860743,2019-05-28 23:41:05,0439
        String[] split = ori.split(",");//切分原始数据

        //截取字段封装相关参数
        String caller = split[0];
        String callee = split[1];
        String buildTime = split[2];
        long time = sdf.parse(buildTime).getTime();
        String buildTime_ts = time + "";
        String duration = split[3];

        //获取分区号
        String regionHash = HBaseUtil.getRegionHash(caller,buildTime,regions);

        //获取rowkey:regionHash_caller_buildTime_callee_flag_duration
        String rowKey = HBaseUtil.genRowKey(regionHash, caller, buildTime, callee, flag, duration);

        //为每一条数据创建put对象
        Put put= new Put(Bytes.toBytes(rowKey));

        //向put中添加数据（列族：列）（值）
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("call1"),Bytes.toBytes(caller));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("buildtime"),Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("buildtime_ts"),Bytes.toBytes(buildTime_ts));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("call2"),Bytes.toBytes(callee));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("flag"),Bytes.toBytes(flag));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("duration"),Bytes.toBytes(duration));

        //向put缓存中添加对象
        putList.add(put);

        //当list中数据条数达到20条，则写入hbase
        if (putList.size()>20){
            table.put(putList);
            //手动提交
            table.flushCommits();
            //清空list集合
            putList.clear();
            //关闭表连接(如果业务单一，可以初始化时创建表连接，此处就不需要关闭表连接)
            table.close();
        }
    }
}
