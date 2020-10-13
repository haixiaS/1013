package kgc.wo824.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class StreamHandler {
    private ICustomerTopology topology;
    Properties prop = new Properties();
    public StreamHandler(ICustomerTopology topology){
        this.topology=topology;
    }
    public void executor(){

        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.43.160:9092");
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "t123");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        Topology topo = this.topology.buildCustomTopology();
        KafkaStreams streams = new KafkaStreams(topo, prop);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("t133") {
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