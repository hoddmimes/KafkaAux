package com.hoddmimes.kafka;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

public class KafkaSubscriber extends Thread
{
    private volatile boolean    mTimeToExit;
    Consumer<Long,String>       mSubscriber;
    SubscriberCallbackInterface mCallback;
    Duration                    mPollDuration;


    KafkaSubscriber( KafkaSubscriberConfig pProperties, SubscriberCallbackInterface pCallback ) {
        mSubscriber = new KafkaConsumer<>( pProperties );
        mCallback = pCallback;
        mTimeToExit = false;
        mPollDuration = Duration.ofMillis(pProperties.getPollInterval());
        this.start();
    }

    public void addSubscription( String pTopic ) {
        mSubscriber.subscribe(Arrays.asList(pTopic));
    }
    public void addSubscription( List<String> pTopics ) {
        mSubscriber.subscribe(pTopics);
    }
    public void addSubscriptionRegExp( String pTopicRegExp ) {
        mSubscriber.subscribe(Pattern.compile( pTopicRegExp ));
    }

    public void close() {
        mTimeToExit = true;
        mSubscriber.close();
    }

    public void run()
    {
        ConsumerRecords<Long,String> tMessages = null;
        Optional<JsonObject> tMsgHdr;

        while(!mTimeToExit) {
            try { tMessages = mSubscriber.poll( mPollDuration ); }
            catch( Throwable pException ) {
                mCallback.subscriberError( pException );
                return;
            }
            for( ConsumerRecord<Long,String> tMsg : tMessages) {
                if ((tMsg.headers() != null) && (tMsg.headers().lastHeader(MsgHeaderItem.KEY) != null)) {
                    tMsgHdr = Optional.of(JsonParser.parseString( new String(tMsg.headers().lastHeader(MsgHeaderItem.KEY).value())).getAsJsonObject());
                } else {
                    tMsgHdr = Optional.empty();
                }
                mCallback.subscriberUpdate( tMsg, tMsgHdr);
            }
        }
    }



}
