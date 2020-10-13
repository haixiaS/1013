package kgc.wo824;

import kgc.wo824.stream.EventTopology;
import kgc.wo824.stream.ICustomerTopology;
import kgc.wo824.stream.StreamHandler;

public class EventDemo {
    public static void main(String[] args) {
        ICustomerTopology topology=new EventTopology();
        StreamHandler handler=new StreamHandler(topology);
        handler.executor();
    }
}
