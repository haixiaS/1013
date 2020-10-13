package kgc.kb07;

import kgc.kb07.commons.HbaseWriter;
import kgc.kb07.commons.IWriter;
import kgc.kb07.handler.*;
import kgc.kb07.worker.HbaseWorker;
import kgc.kb07.worker.ParentWorker;

public class HbaseWorkerTest {
    public static void main(String[] args) {
        //user_friends
       IParseRecord userFriendsHander=new UserFriendHandler();
        IWriter writer=new HbaseWriter(userFriendsHander,"events_db:user_friend");
        ParentWorker worker=new HbaseWorker(writer);
        worker.setTopicName("friend");
        worker.fillData();

        //events
        /*IParseRecord userFriendsHander=new EventsHandler();
        IWriter writer=new HbaseWriter(userFriendsHander,"events_db:events");
        ParentWorker worker=new HbaseWorker(writer);
        worker.setTopicName("event");
        worker.fillData();*/

        //event_attend
       /* IParseRecord eventHandler=new EventHandler();
        IWriter writer=new HbaseWriter(eventHandler,"events_db:event_attendee");
        ParentWorker worker=new HbaseWorker("i",writer);
        worker.setTopicName("invited");
        worker.fillData();*/
       /*//users
        IParseRecord userHandler=new UserHandler();
        IWriter writer=new HbaseWriter(userHandler,"events_db:users");
        ParentWorker worker=new HbaseWorker(writer);
        worker.setTopicName("users");
        worker.fillData();*/

      /* //train
        IParseRecord trainHandler=new TrainHandler();
        IWriter writer=new HbaseWriter(trainHandler,"events_db:train");
        ParentWorker worker=new HbaseWorker(writer);
        worker.setTopicName("train");
        worker.fillData();*/

        //test
        /*IParseRecord testHandler=new TestHandler();
        IWriter writer=new HbaseWriter(testHandler);
        ParentWorker worker=new HbaseWorker("t1",writer);
        worker.setTopicName("test");
        worker.fillData("tests:test");*/


    }
}
