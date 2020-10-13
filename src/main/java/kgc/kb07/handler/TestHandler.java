package kgc.kb07.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class TestHandler implements IParseRecord{
    @Override
    public List<Put> parse(ConsumerRecords<String, String> records) {
        List<Put> putlist=new ArrayList<>();
        for (ConsumerRecord<String,String> record:records) {
            System.out.println(record);
            String[] infos = record.value().split(",",-1);
            Put put = new Put(Bytes.toBytes((infos[0] + infos[1]+ infos[2]+ infos[3]).hashCode()));
            put.addColumn("tf".getBytes(),"user".getBytes(),infos[0].getBytes());
            put.addColumn("tf".getBytes(),"event".getBytes(),infos[1].getBytes());
            put.addColumn("tf".getBytes(),"invited".getBytes(),infos[2].getBytes());
            put.addColumn("tf".getBytes(),"timestamp".getBytes(),infos[3].getBytes());
            putlist.add(put);
        }
        return putlist;
    }
}
