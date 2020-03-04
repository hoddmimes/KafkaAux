package com.hoddmimes.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Properties;

public class KafkaSubscriberConfig extends Properties
{
    public static final String POLL_INTERVAL = "poll_interval";

    public KafkaSubscriberConfig() {
        this("localhost:9092");
    }


    public KafkaSubscriberConfig(String pBootstrapServerPort) {
        super();
        this.put("bootstrap.servers", pBootstrapServerPort);
        this.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, pBootstrapServerPort);
        this.put(ConsumerConfig.GROUP_ID_CONFIG, "1");
        this.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        this.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        this.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        this.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        this.setProperty(POLL_INTERVAL, String.valueOf(20));
    }

    public void setPollInterval( long pInterval ) {
        this.setProperty(POLL_INTERVAL, String.valueOf(pInterval));
    }

    public long getPollInterval() {
        try {
            return Long.parseLong(this.getProperty(POLL_INTERVAL,"50"));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        return 50L;
    }

}
