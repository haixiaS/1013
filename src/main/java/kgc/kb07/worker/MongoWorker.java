package kgc.kb07.worker;


import kgc.kb07.commons.IWriter;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;

public class MongoWorker extends ParentWorker {
    private IWriter writer;


    public MongoWorker(String groupId, String resetCfg, String autoCommit, IWriter writer) {
        super(groupId, resetCfg, autoCommit);
        this.writer = writer;
    }

    public MongoWorker(IWriter writer) {
        this.writer = writer;
    }

    public MongoWorker(String groupId, IWriter writer) {
        super(groupId);
        this.writer = writer;
    }
    public MongoWorker(String groupId, String autoCommit, IWriter writer) {
        super(groupId,autoCommit);
        this.writer = writer;
    }

    @Override
    public void fillData() {
        KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(prop);
        consumer.subscribe(Collections.singletonList(super.topicName));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(super.poolTime);
                writer.write(records);
                if (records.count()<=0){
                    System.out.println("finished");
                    break;
                }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
