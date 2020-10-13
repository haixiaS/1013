package kgc.kb07.handler;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class TrainHandlerM implements IParseRecordsM{
    @Override
    public List<Document> parse(ConsumerRecords<String, String> records) {
        List<Document> putlist=new ArrayList<>();
        for (ConsumerRecord<String,String> record:records) {
            //System.out.println(record);
            String[] infos = record.value().split(",",-1);
            Document doc=new Document();
            doc.append("user",infos[0]).append("event",infos[1]).append("invited",infos[2]).append("timestamp",infos[3]).append("interested",infos[4]).append("not_interested",infos[5]);
            putlist.add(doc);
            System.out.println(doc.toJson());
        }
        return putlist;
    }
}

