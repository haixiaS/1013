package kgc.wo824;

import kgc.wo824.stream.ICustomerTopology;
import kgc.wo824.stream.StreamHandler;
import kgc.wo824.stream.UserFriendTopology;

public class UserFriendDemo {
    public static void main(String[] args) {
        ICustomerTopology topology=new UserFriendTopology();
        StreamHandler handler=new StreamHandler(topology);
        handler.executor();
    }
}
