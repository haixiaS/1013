package kgc.kb07.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class UserHandlerM implements IParseRecordsM{
    @Override
    public List<Document> parse(ConsumerRecords<String, String> records) {
        List<Document> putlist=new ArrayList<>();
        for (ConsumerRecord<String,String> record:records) {
            String[] infos = record.value().split(",",-1);
            Document doc=new Document();
            doc.append("user_id",infos[0]).append("local",infos[1]).append("birth_year",infos[2]).append("gender",infos[3]).append("joined_at",infos[4])
                    .append("location",infos[5]).append("timezone",infos[6]);
            putlist.add(doc);
            System.out.println(doc.toJson());
        }
        return putlist;
    }
}

