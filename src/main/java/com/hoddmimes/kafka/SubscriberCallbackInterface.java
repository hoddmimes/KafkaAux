package com.hoddmimes.kafka;

import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Optional;

public interface SubscriberCallbackInterface
{
    public void subscriberUpdate( ConsumerRecord<Long, String> pUpdate, Optional<JsonObject> jMsgHdr );
    public void subscriberError(Throwable pException );
}
