package kgc.kb07.worker;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public abstract class ParentWorker implements IWorker {
    Properties prop=new Properties();
    protected Long poolTime=500L;//默认值
    protected String topicName;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public ParentWorker(String groupId, String resetCfg, String autoCommit) {

        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.43.160:9092");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,autoCommit);//关闭自动提交
        prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,resetCfg);//默认从最新的读，设置开始读取的位置
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);//分组名，可以随便给
    }

    //public ParentWorker() {
 /*       //自己调自己的方法
        this("demo1", "earliest", "false");
    }
    public ParentWorker(String groupId) {
        //自己调自己的方法
        this(groupId, "earliest", "false");
    }
   *//* public ParentWorker(String resetCfg) {
        //自己调自己的方法
        this("demo1", resetCfg, "false");
    }
    public ParentWorker(String autoCommit) {
        //自己调自己的方法
        this("demo1", "earliest", autoCommit);
    }*/

    public ParentWorker(){
        this("demo3","earliest","false");
    }
    public ParentWorker(String groupId){
        this(groupId,"earliest","false");
    }
    public ParentWorker(String resetCfg,String autoCommit){
        this("demo4",resetCfg,autoCommit);
    }
}

