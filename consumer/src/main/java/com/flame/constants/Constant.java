package com.flame.constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;


public interface Constant {
    Configuration CONF = HBaseConfiguration.create();
}
