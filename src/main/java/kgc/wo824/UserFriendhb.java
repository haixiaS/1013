package kgc.wo824;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
/*import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;*/
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

//将kafka topic user_friends 导入到hbase event_db:events
public class UserFriendhb {
    public static void main(String[] args) {
        //kafka
        //生产者序列化，消费者反序列化
        Properties prop=new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.43.160:9092");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);//关闭自动提交
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//默认从最新的读，设置开始读取的位置
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,"att");//分组名，可以随便给
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
        consumer.subscribe(Collections.singletonList("invited"));//读取的topic

        //hbase
        Configuration config= HBaseConfiguration.create();
        config.set("hbase.rootdir","hdfs://192.168.43.160:9000/hbase");
        config.set("hbase.zookeeper.quorum","192.168.43.160");
        config.set("hbase.zookeeper.property.clientPort","2181");
        try {

            Connection connection = ConnectionFactory.createConnection(config);
            Table table = connection.getTable(TableName.valueOf("at1:at"));
            while (true){
                ConsumerRecords<String,String> records=consumer.poll(1000);
//                for (ConsumerRecord<String,String> record:records) {
//                    System.out.println(record);
//                    String[] infos = record.value().split(" ");
//                }
              /* List<Put> putlist=new ArrayList<>();
               for (ConsumerRecord<String,String> record:records) {
                   System.out.println(record);
                  String[] infos = record.value().split(" ",-1);
                   Put put = new Put(Bytes.toBytes((infos[0] + infos[1]).hashCode()));
                   put.addColumn("uf".getBytes(),"userid".getBytes(),infos[0].getBytes());
                   put.addColumn("uf".getBytes(),"friendid".getBytes(),infos[1].getBytes());
                   putlist.add(put);
                }*/



                List<Put> putlist=new ArrayList<>();
              for (ConsumerRecord<String,String> record:records) {
            System.out.println(record);
            String[] infos = record.value().split(",",-1);
            Put put = new Put(Bytes.toBytes((infos[0] + infos[1] + infos[2]).hashCode()));
            put.addColumn("ae".getBytes(),"event".getBytes(),infos[0].getBytes());
            put.addColumn("ae".getBytes(),"invited".getBytes(),infos[1].getBytes());
            put.addColumn("ae".getBytes(),"answer".getBytes(),infos[2].getBytes());
            putlist.add(put);
        }

               table.put(putlist);
               Thread.sleep(200);
               table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
