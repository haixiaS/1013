package kgc.kb07.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.bson.Document;

import java.util.List;

public interface IParseRecordsM {
    public List<Document> parse(ConsumerRecords<String,String> records);
}
