package com.flame.coprocessor;

import com.flame.util.ConnectionInstance;
import com.flame.util.HBaseUtil;
import com.flame.util.PropertyUtil;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;


public class CalleeWriteObserver extends BaseRegionObserver {

    private int regions = Integer.valueOf(PropertyUtil.properties.getProperty("hbase.regions"));
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //获取之前操作的表
        String tableName  = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        String curTableName = PropertyUtil.properties.getProperty("hbase.table.name");
        if (!tableName.equals(curTableName)) return;
        //获取之前数据的rowkey
        String row = Bytes.toString(put.getRow());

        //00_15961260091_20190305 10:04:05_13157770954_1_0673
        String[] splits = row.split("_");
        String flag  = splits[4];
        if ("0".equals(flag)){
            return;
        }
        String caller = splits[1];
        String buildTime = splits[2];
        String buildTime_ts = null;
        try {
            buildTime_ts = sdf.parse(buildTime).getTime()+"";
        } catch (ParseException e1) {
            e1.printStackTrace();
        }
        String callee = splits[3];
        String duration = splits[5];

        //获取分区号
        String regionHash = HBaseUtil.getRegionHash(callee,buildTime,regions);
        //获取rowkey
        String rowkey = HBaseUtil.genRowKey(regionHash,callee,buildTime,caller,"0",duration);

        Put newPut = new Put(Bytes.toBytes(rowkey));
        newPut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("call1"),Bytes.toBytes(callee));
        newPut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("buildtime"),Bytes.toBytes(buildTime));
        newPut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("buildtime_ts"),Bytes.toBytes(buildTime_ts));
        newPut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("call2"),Bytes.toBytes(caller));
        newPut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("flag"),Bytes.toBytes("0"));
        newPut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("duration"),Bytes.toBytes(duration));

        //获取连接
        Connection connection = ConnectionInstance.getInstance();
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(newPut);
        table.close();
    }
}
