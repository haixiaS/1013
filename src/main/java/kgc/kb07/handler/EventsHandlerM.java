package kgc.kb07.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class EventsHandlerM implements IParseRecordsM{
    @Override
        public List<Document> parse(ConsumerRecords<String, String> records) {
            List<Document> putlist=new ArrayList<>();
            for (ConsumerRecord<String,String> record:records) {
                String[] infos = record.value().split(",",-1);
                Document doc = new Document();
                doc.append("event_id", infos[0]).append("start_time", infos[1]).append("city", infos[3]).append("state", infos[4]).append("zip", infos[5])
                        .append("country", infos[6])
                        .append("lat", infos[7]).append("lng", infos[8]).append("user_id", infos[1]).append("common_words", infos[9]);
                // System.out.println(infos[0].concat(infos[1]));
                // System.out.println(doc.toJson());
                putlist.add(doc);
                System.out.println(doc.toJson());
            }
            return putlist;
        }
    }
