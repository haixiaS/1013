package kgc.kb07.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class UtilsConfig {
    public static Configuration getHbaseConf(){
        Configuration config= HBaseConfiguration.create();
        config.set("hbase.rootdir","hdfs://192.168.43.160:9000/hbase");
        config.set("hbase.zookeeper.quorum","192.168.43.160");
        config.set("hbase.zookeeper.property.clientPort","2181");
        return  config;
    }
}
