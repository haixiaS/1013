package kgc.kb07.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class EventHandler implements IParseRecord{
    @Override
    public List<Put> parse(ConsumerRecords<String, String> records) {
        List<Put> putlist=new ArrayList<>();
        for (ConsumerRecord<String,String> record:records) {
            System.out.println(record);
            String[] infos = record.value().split(",",-1);
            Put put = new Put(Bytes.toBytes((infos[0] + infos[1]).hashCode()));
            put.addColumn("euat".getBytes(),"event_id".getBytes(),infos[0].getBytes());
            put.addColumn("euat".getBytes(),"user_id".getBytes(),infos[1].getBytes());
            put.addColumn("euat".getBytes(),"attend_type".getBytes(),infos[2].getBytes());
            putlist.add(put);
        }
        return putlist;
    }
}
