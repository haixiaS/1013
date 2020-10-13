package kgc.wo824;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class MyUserFriends {
    public static void main(String[] args) {
       /* Properties prop=new Properties();
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.43.100:9092");
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG,"kb07");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        StreamsBuilder builder=new StreamsBuilder();
        //目标指向
       KStream<Object, Object> user_friends_raw = builder.stream(" user_friend_raw")
                .filter((k, v) -> (!v.toString().startsWith("user")
                        && v.toString().split(",").length == 2));
       user_friends_raw.flatMap((k,v)->{
           System.out.println(k+"***********"+v);
           List<KeyValue<String,String>> keyValues=new ArrayList<>();
           String[] split = v.toString().split(",");
           String userId = split[0];
           String[] friends = split[1].split(" ");
           for (String friend:friends) {
               KeyValue<String, String> keyValue = new KeyValue<>(null, userId + " " + friend);
               keyValues.add(keyValue);
           }
           return keyValues;
       }).to("user_friends1");//存到user_friends
        //构建拓扑结构
        Topology topo = builder.build();
        KafkaStreams streams = new KafkaStreams(topo, prop);
        CountDownLatch countDownLatch=new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("kb"){
            @Override
            public void run() {
                streams.close();
                countDownLatch.countDown();
            }
        });

        try {
         streams.start();
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.exit(0);*/
     Properties prop = new Properties();
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.43.160:9092");
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG,"KB0722");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Object, Object> user_friend_raw = builder.stream("user_friends_raw")
                .filter((k, v) -> (!v.toString().startsWith("user") && v.toString().split(",").length == 2));

        user_friend_raw.flatMap((k, v) -> {
            System.out.println(k + "  " + v);
            List<KeyValue<String, String>> keyValues = new ArrayList<>();
            String[] split = v.toString().split(",");
            String userId = split[0];
            String[] friends = split[1].split(" ");
            for (String friend : friends) {
                KeyValue<String, String> keyValue = new KeyValue<>(null, userId + " " + friend);
                keyValues.add(keyValue);
            }
            return keyValues;
        }).to("user_friends");

        Topology topo = builder.build();
        KafkaStreams streams = new KafkaStreams(topo, prop);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("kb07") {
            @Override
            public void run() {
                streams.close();
                countDownLatch.countDown();
            }
        });


        try {
            streams.start();
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

}
