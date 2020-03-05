package com.hoddmimes.kafka;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Properties;

public class KafkaPublisherConfig extends Properties
{

    public KafkaPublisherConfig() {
        this("localhost:9092");
    }



    public KafkaPublisherConfig( String pBootstrapServerPort) {
        super();
        this.put("bootstrap.servers", pBootstrapServerPort);
        this.put("enable.idempotence=",true);
        this.put("acks", "all");
        this.put("retries", 1000000);
        this.put("batch.size", 16384);
        this.put("linger.ms", 1);
        this.put("client.id","");
        this.put("buffer.memory", 33554432);
        this.put("key.serializer", LongSerializer.class.getName());
        this.put("value.serializer", ByteArraySerializer.class.getName());
    }

}
