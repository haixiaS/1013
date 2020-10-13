package kgc.kb07.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class EventAttendHandlerM implements IParseRecordsM{
    @Override
    public List<Document> parse(ConsumerRecords<String, String> records) {
        List<Document> putlist = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
           // System.out.println(record);
            String[] infos = record.value().split(",", -1);
            Document doc = new Document();

            doc.append("event_id", infos[0]).append("user_id", infos[1]).append("attend_type", infos[2]);
            // System.out.println(infos[0].concat(infos[1]));
            // System.out.println(doc.toJson());
            putlist.add(doc);
            System.out.println(doc.toJson());
        }
        return putlist;
    }
}
