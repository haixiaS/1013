package kgc.wo824.stream;

import org.apache.kafka.streams.Topology;

public interface ICustomerTopology {
    public Topology buildCustomTopology();
}
