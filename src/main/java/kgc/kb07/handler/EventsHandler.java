package kgc.kb07.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class EventsHandler implements IParseRecord{
    @Override
        public List<Put> parse(ConsumerRecords<String, String> records) {
            List<Put> putlist=new ArrayList<>();
            for (ConsumerRecord<String,String> record:records) {
                System.out.println(record);
                String[] infos = record.value().split(",",-1);
                Put put = new Put(Bytes.toBytes((infos[0])));
                put.addColumn("schedule".getBytes(),"start_time".getBytes(),infos[2].getBytes());
                put.addColumn("location".getBytes(),"city".getBytes(),infos[3].getBytes());
                put.addColumn("location".getBytes(),"state".getBytes(),infos[4].getBytes());
                put.addColumn("location".getBytes(),"zip".getBytes(),infos[5].getBytes());
                put.addColumn("location".getBytes(),"country".getBytes(),infos[6].getBytes());
                put.addColumn("location".getBytes(),"lat".getBytes(),infos[7].getBytes());
                put.addColumn("location".getBytes(),"lng".getBytes(),infos[8].getBytes());
                put.addColumn("creator".getBytes(),"user_id".getBytes(),infos[1].getBytes());
                put.addColumn("remark".getBytes(),"common_words".getBytes(),infos[9].getBytes());
                putlist.add(put);
            }
            return putlist;
        }
    }
