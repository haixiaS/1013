package kgc.kb07.commons;

import org.apache.kafka.clients.consumer.ConsumerRecords;

//公共类，作用定义kafka写入到目标数据库的行为方式,定义为接口
public interface IWriter {
    public int write(ConsumerRecords<String,String> records);}
