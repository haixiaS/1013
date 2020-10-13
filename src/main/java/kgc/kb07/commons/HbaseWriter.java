package kgc.kb07.commons;




import kgc.kb07.handler.IParseRecord;
import kgc.kb07.utils.UtilsConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.util.List;

public class HbaseWriter implements IWriter {
    private IParseRecord record;
    private Table table;
    private String tableName;

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setRecord(IParseRecord record) {
        this.record = record;
    }

    public HbaseWriter(IParseRecord record,String tableName) {
        this.record = record;
        this.tableName = tableName;
        Configuration config= UtilsConfig.getHbaseConf();
        try {
            Connection connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int write(ConsumerRecords<String, String> records) {
        List<Put> putList=null;
        try {
            putList=record.parse(records);
            table.put(putList);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
