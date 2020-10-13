package kgc.wo824.stream;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.ArrayList;
import java.util.List;

public class EventTopology implements ICustomerTopology{
    @Override
    public Topology buildCustomTopology() {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Object, Object> attendee = builder.stream("event_attendees_raw")
                .filter((k, v) -> (!v.toString().startsWith("event") && v.toString().split(",").length == 5));
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
        }).to("event_attendees");

        Topology topo = builder.build();
        return topo;
    }
}
