package kgc.kb07.commons;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import kgc.kb07.handler.IParseRecordsM;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MongoDBWriter implements IWriter {
    private IParseRecordsM record;
    private String tName;
    private MongoCollection<Document> table;

    public void setRecord(IParseRecordsM record) {
        this.record = record;
    }

    public void settName(String tName) {
        this.tName = tName;
    }

    public MongoDBWriter(IParseRecordsM record, String tName) {
        this.record = record;
        this.tName = tName;
        ServerAddress serverAddress= new ServerAddress("192.168.43.160",27017);
        List<ServerAddress> list=new ArrayList<>();
        list.add(serverAddress);
        //设置mongoDB
        MongoCredential credential=MongoCredential.createScramSha1Credential("gree","events_db","gree".toCharArray());
        List<MongoCredential> addr=new ArrayList<>();
        addr.add(credential);
        MongoClient mongoClient=new MongoClient(list,addr);
        MongoDatabase kb07DB=mongoClient.getDatabase("events");
        table=kb07DB.getCollection(tName);
    }

    @Override
    public int write(ConsumerRecords<String, String> records) {
        List<Document> docList=null;
        docList=record.parse(records);
        System.out.println(docList.size());
        if (docList.size()>0){
        table.insertMany(docList);
        }
        return docList.size();
    }
}
