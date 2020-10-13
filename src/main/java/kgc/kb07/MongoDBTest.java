package kgc.kb07;

import kgc.kb07.commons.IWriter;
import kgc.kb07.commons.MongoDBWriter;
import kgc.kb07.handler.*;
import kgc.kb07.worker.MongoWorker;
import kgc.kb07.worker.ParentWorker;

public class MongoDBTest {
    public static void main(String[] args) {
        //userfriends
       /* IParseRecordsM userFriendsHander=new UserFriendHandlerMg();
        IWriter writer=new MongoDBWriter(userFriendsHander,"user_friend");
        ParentWorker worker=new MongoWorker(writer);
        worker.setTopicName("user_friends");
        worker.fillData();*/

        //events
       /* IParseRecordsM events=new EventsHandlerM();
        IWriter writerEvents=new MongoDBWriter(events,"events");
        ParentWorker workerEvents=new MongoWorker(writerEvents);
        workerEvents.setTopicName("events");
        workerEvents.fillData();*/

        //attends
       /* IParseRecordsM attends=new EventAttendHandlerM();
        IWriter writerAttends=new MongoDBWriter(attends,"event_attendees");
        ParentWorker workerAttends=new MongoWorker(writerAttends);
        workerAttends.setTopicName("event_attendees");
        workerAttends.fillData();*/
        //users
        IParseRecordsM users=new UserHandlerM();
        IWriter writerUsers=new MongoDBWriter(users,"users");
        ParentWorker workerUsers=new MongoWorker(writerUsers);
        workerUsers.setTopicName("users");
        workerUsers.fillData();

        //Train
       /* IParseRecordsM train=new TrainHandlerM();
        IWriter writerTrain=new MongoDBWriter(train,"train");
        ParentWorker workerTrain=new MongoWorker(writerTrain);
        workerTrain.setTopicName("train");
        workerTrain.fillData();*/
    }
}
