package kgc.wo824;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class Users {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.43.160:9092");
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "user");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Object, Object> attendee = builder.stream("users")
                .filter((k, v) -> (!v.toString().startsWith("user") && v.toString().split(",").length == 7));
       //event	yes	maybe	invited	no
        attendee.flatMap((k, v) -> {
            System.out.println(k + "  " + v);
            List<KeyValue<String, String>> keyValues = new ArrayList<>();
            String[] split = v.toString().split(",");
            String event = split[0];
            String[] yesS = split[1].split(" ");
            for (String yes : yesS) {
                KeyValue<String, String> keyValue = new KeyValue<>(null, event+","+yes+",yes");
                keyValues.add(keyValue);
            }
            String[] maybeS = split[2].split(" ");
            for (String maybe : maybeS) {
                KeyValue<String, String> keyValue = new KeyValue<>(null, event+","+maybe+",mabye");
                keyValues.add(keyValue);
            }
            String[] invitedS = split[3].split(" ");
            for (String invited : invitedS) {
                KeyValue<String, String> keyValue = new KeyValue<>(null, event+","+invited+",invited");
                keyValues.add(keyValue);
            }
            String[] noS = split[4].split(" ");
            for (String no : noS) {
                KeyValue<String, String> keyValue = new KeyValue<>(null, event+","+no+",no");
                keyValues.add(keyValue);
            }
            return keyValues;
    }).to("attends");

        Topology topo = builder.build();
        KafkaStreams streams = new KafkaStreams(topo, prop);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("t1") {
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
