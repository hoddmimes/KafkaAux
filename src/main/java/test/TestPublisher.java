package test;

import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.google.gson.JsonObject;
import com.hoddmimes.kafka.KafkaPublisher;
import com.hoddmimes.kafka.KafkaPublisherConfig;
import org.apache.kafka.common.KafkaException;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class TestPublisher
{
    private static final SimpleDateFormat SDF = new SimpleDateFormat("HH:mm:ss.SSS");

    private int mMsgsToSend  = 10000;
    private int mMinMsgSize  = 20;
    private int mMaxMsgSize = 1024;
    private double mMsgRate = 10;
    private int mNumberOfTopics = 100;
    private long mHoldback = 0;
    private int  mBatchFactor = 1;
    private String mProducerId = null;
    private List<Topic> mTopics;

    private KafkaPublisher mPublisher;

    public static void main( String [] args )
    {
        TestPublisher tPublisher = new TestPublisher();
        tPublisher.parseArguments( args );
        tPublisher.initialize();
        tPublisher.publish();
    }

    private String generateData( int pSize ) {
        byte[] tBuffer = new byte[ pSize ];
        for( int i = 0; i < pSize; i++ ) {
            tBuffer[i] = (byte) (65 + (i%25));
        }
        return new String(tBuffer);
    }

    JsonObject buildMessage(String pTopic, long pSeqNo, int pDataSize ) {
        JsonObject tMsg = new JsonObject();
        tMsg.addProperty("topic", pTopic);
        tMsg.addProperty("seqno", pSeqNo);
        tMsg.addProperty("time", SDF.format( System.currentTimeMillis()));
        tMsg.addProperty("data", generateData( pDataSize ));
        return tMsg;
    }



    private void publish() {
        int i = 0;
        long tStartTime = System.currentTimeMillis();
        Random rnd = new Random();
        while( i < mMsgsToSend ) {
            for( int j = 0; j < mBatchFactor; j++) {
                Topic tTopic = mTopics.get(rnd.nextInt( mTopics.size()));
                int tSize = rnd.nextInt( mMaxMsgSize - mMinMsgSize) + mMinMsgSize;
                tTopic.mSeqNo.incrementAndGet();
                try {mPublisher.send( tTopic.mTopic, 0, tTopic.mSeqNo.longValue(), buildMessage( tTopic.mTopic, tTopic.mSeqNo.longValue(), tSize ));}
                catch( KafkaException e) {
                    e.printStackTrace();
                }
            }
            i += mBatchFactor;
            if ((i % 250) == 0) {
                System.out.println("Sent " + i + " messages");
            }
            try { Thread.sleep( mHoldback ); }
            catch( InterruptedException e) {}
        }
        double execTime = (double) System.currentTimeMillis() - tStartTime;
        double tRate = (mMsgsToSend * 1000) / execTime;
        System.out.println( String.format("%d msgs sent, rate: %6.2f exec time: %9.2f",
                    mMsgsToSend, tRate, (execTime/1000)));
    }

    private void initialize() {
        KafkaPublisherConfig tConfig = new KafkaPublisherConfig();
        mPublisher = new KafkaPublisher( tConfig );

        mTopics = new ArrayList<>( mNumberOfTopics );
        for( int i = 0; i < mNumberOfTopics; i++) {
            mTopics.add( new Topic( String.format("topic-%04d", (i+1))));
        }

        if (mMsgRate <= 100) {
            mBatchFactor = 1;
            mHoldback = (long) (1000 / mMsgRate);
        } else if (mMsgRate <= 1000) {
            mBatchFactor = 10;
            mHoldback = (long) ((mBatchFactor * 1000) / mMsgRate);
        } else if (mMsgRate <= 5000) {
            mBatchFactor = 25;
            mHoldback = (long) ((mBatchFactor * 1000) / mMsgRate);
        } else {
            mBatchFactor = 100;
            mHoldback = (long) ((mBatchFactor * 1000) / mMsgRate);
        }
    }

    private void parseArguments( String[] args ) {
        int i = 0;
        while( i < args.length ) {
            if (args[i].compareToIgnoreCase("-id") == 0) {
                mProducerId =  args[i+1];
                i++;
            }
            if (args[i].compareToIgnoreCase("-count") == 0) {
                mMsgsToSend =  Integer.parseInt( args[i+1] );
                i++;
            }
            if (args[i].compareToIgnoreCase("-minSize") == 0) {
                mMinMsgSize = Integer.parseInt( args[i+1] );
                i++;
            }
            if (args[i].compareToIgnoreCase("-maxSize") == 0) {
                mMaxMsgSize = Integer.parseInt( args[i+1] );
                i++;
            }
            if (args[i].compareToIgnoreCase("-rate") == 0) {
                mMsgRate = Integer.parseInt( args[i+1] );
                i++;
            }
            if (args[i].compareToIgnoreCase("-topics") == 0) {
                mNumberOfTopics = Integer.parseInt( args[i+1] );
                i++;
            }
            i++;
        }
    }

    class Topic {
        private final String mTopic;
        private AtomicLong mSeqNo;

        Topic( String pTopic ) {
            mTopic = pTopic;
            mSeqNo = new AtomicLong(0);
        }
    }
}
