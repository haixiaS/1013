package kgc.wo824.stream;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.ArrayList;
import java.util.List;

public class UserFriendTopology implements ICustomerTopology {
    @Override
    public Topology buildCustomTopology() {
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
        return topo;
    }
}
