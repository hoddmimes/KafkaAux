package com.hoddmimes.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class KafkaSubscriber extends Thread
{
    private volatile boolean    mTimeToExit;
    Consumer<Long,byte[]>       mSubscriber;
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
        ConsumerRecords<Long,byte[]> tMessages = null;

        while(!mTimeToExit) {
            try { tMessages = mSubscriber.poll( mPollDuration ); }
            catch( Throwable pException ) {
                mCallback.subscriberError( pException );
                return;
            }
            for( ConsumerRecord<Long,byte[]> pMsg : tMessages) {
                mCallback.subscriberUpdate( pMsg );
            }
        }
    }



}
