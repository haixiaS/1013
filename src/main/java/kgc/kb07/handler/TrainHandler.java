package kgc.kb07.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class TrainHandler implements IParseRecord{
    @Override
    public List<Put> parse(ConsumerRecords<String, String> records) {
        List<Put> putlist=new ArrayList<>();
        for (ConsumerRecord<String,String> record:records) {
            System.out.println(record);
            String[] infos = record.value().split(",",-1);
            Put put = new Put(Bytes.toBytes((infos[0] + infos[1]).hashCode()));
            put.addColumn("eu".getBytes(),"user".getBytes(),infos[0].getBytes());
            put.addColumn("eu".getBytes(),"event".getBytes(),infos[1].getBytes());
            put.addColumn("eu".getBytes(),"invited".getBytes(),infos[2].getBytes());
            put.addColumn("eu".getBytes(),"timestamp".getBytes(),infos[3].getBytes());
            put.addColumn("eu".getBytes(),"interested".getBytes(),infos[4].getBytes());
            put.addColumn("eu".getBytes(),"not_interested".getBytes(),infos[5].getBytes());
            putlist.add(put);
        }
        return putlist;
    }
}
