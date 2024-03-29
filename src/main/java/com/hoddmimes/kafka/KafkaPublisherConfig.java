package com.hoddmimes.kafka;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaPublisherConfig extends Properties
{

    public KafkaPublisherConfig() {
        this("publisher","localhost:9092");
    }



    public KafkaPublisherConfig( String pClientId, String pBootstrapServerPort) {
        super();
        this.put("bootstrap.servers", pBootstrapServerPort);
        //this.put("enable.idempotence=",false);
        this.put("auto.create.topics.enable", true);
        this.put("acks", "all");
        this.put("retries", 0);
        //this.put("request.timeout.ms", 1000);
        this.put("batch.size", 16384);
        this.put("linger.ms", 0);
        //this.put("client.id", pClientId);
        this.put("buffer.memory", 33554432);
        this.put("key.serializer", LongSerializer.class.getName());
        this.put("value.serializer", StringSerializer.class.getName());
    }

    public void setHoldback( long pMillisec) {
        this.put("linger.ms", pMillisec);
    }


}
