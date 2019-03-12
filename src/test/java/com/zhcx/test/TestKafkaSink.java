package com.zhcx.test;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.robin.etl.kafka.sink.CommKafkaPartitionSink;

import java.util.ResourceBundle;
import java.util.concurrent.Executors;

/**
 * <p>Project:  kafka-sink</p>
 * <p>
 * <p>Description:com.zhcx.test</p>
 * <p>
 * <p>Copyright: Copyright (c) 2019 create at 2019年03月11日</p>
 * <p>
 * <p>Company: zhcx_DEV</p>
 *
 * @author robinjim
 * @version 1.0
 */
public class TestKafkaSink {
    public static void main(String[] args){
        ListeningExecutorService scanjobPool= MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
        CommKafkaPartitionSink sink=new CommKafkaPartitionSink(ResourceBundle.getBundle("testconf"));
        scanjobPool.submit(sink);


    }
}
