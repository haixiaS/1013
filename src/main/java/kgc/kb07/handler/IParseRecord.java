
package kgc.kb07.handler;
import org.apache.hadoop.hbase.client.Put;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.List;

public interface IParseRecord {
    public List<Put> parse(ConsumerRecords<String,String> records);
}
