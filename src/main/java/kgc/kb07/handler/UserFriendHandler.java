package kgc.kb07.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class UserFriendHandler implements IParseRecord{
    @Override
    public List<Put> parse(ConsumerRecords<String, String> records) {
        List<Put> putlist=new ArrayList<>();
        for (ConsumerRecord<String,String> record:records) {
            System.out.println(record);
            String[] infos = record.value().split(" ",-1);
            Put put = new Put(Bytes.toBytes((infos[0] + infos[1]).hashCode()));
            put.addColumn("uf".getBytes(),"userid".getBytes(),infos[0].getBytes());
            put.addColumn("uf".getBytes(),"friendid".getBytes(),infos[1].getBytes());
            putlist.add(put);
        }
        return putlist;
    }
}
