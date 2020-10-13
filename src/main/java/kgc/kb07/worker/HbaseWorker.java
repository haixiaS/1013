package kgc.kb07.worker;


import kgc.kb07.commons.IWriter;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;

public class HbaseWorker extends ParentWorker {
    private IWriter writer;


    public HbaseWorker(String groupId, String resetCfg, String autoCommit, IWriter writer) {
        super(groupId, resetCfg, autoCommit);
        this.writer = writer;
    }

    public HbaseWorker(IWriter writer) {
        this.writer = writer;
    }

    public HbaseWorker(String groupId, IWriter writer) {
        super(groupId);
        this.writer = writer;
    }
    public HbaseWorker(String groupId,String autoCommit, IWriter writer) {
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
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
