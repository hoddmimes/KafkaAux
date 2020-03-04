package com.hoddmimes.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.time.Duration;

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

    public void close() {
        mTimeToExit = true;
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
