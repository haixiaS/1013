package kgc.kb07.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class UserHandler implements IParseRecord{
    @Override
    public List<Put> parse(ConsumerRecords<String, String> records) {
        List<Put> putlist=new ArrayList<>();
        for (ConsumerRecord<String,String> record:records) {
            System.out.println(record);
            String[] infos = record.value().split(",",-1);
            Put put = new Put(Bytes.toBytes((infos[0]).hashCode()));
            put.addColumn("region".getBytes(),"local".getBytes(),infos[1].getBytes());
            put.addColumn("profile".getBytes(),"birthyear".getBytes(),infos[2].getBytes());
            put.addColumn("profile".getBytes(),"gender".getBytes(),infos[3].getBytes());
            if(infos.length>5){
                put.addColumn("region".getBytes(),"location".getBytes(),infos[5].getBytes());
            }
            if(infos.length>6){
                put.addColumn("region".getBytes(),"timezone".getBytes(),infos[6].getBytes());
            }
            put.addColumn("registration".getBytes(),"joinedAt".getBytes(),infos[4].getBytes());
            putlist.add(put);
        }
        return putlist;
    }
}
