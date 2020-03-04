package com.hoddmimes.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;

public interface SubscriberCallbackInterface
{
    public void subscriberUpdate( ConsumerRecord<Long, byte[]> pUpdate );
    public void subscriberError(Throwable pException );
}
