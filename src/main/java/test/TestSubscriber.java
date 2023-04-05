package test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.hoddmimes.kafka.KafkaSubscriber;
import com.hoddmimes.kafka.KafkaSubscriberConfig;
import com.hoddmimes.kafka.SubscriberCallbackInterface;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class TestSubscriber implements SubscriberCallbackInterface {
    private static SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");


    private enum LOG_MODE {Normal, Verbose};

    private KafkaSubscriber mSubscriber;
    private String mBootServer = "localhost:9092";
    private LOG_MODE mLogMode = LOG_MODE.Verbose;
    private boolean mCheckSeqNo = true;
    private boolean mAutoCommit = true;
    private List<Pattern> mSubscriptionTopics;
    private Map<String,Topic> mTopics = new HashMap<>();
    private Gson mPrettyPrinter = new GsonBuilder().setPrettyPrinting().create();
    private long mTotUpdates = 0;


    public static void main(String[] args) {
        TestSubscriber subscr = new TestSubscriber();
        subscr.parseArguments(args);
        subscr.execute();
    }

    private void execute( ) {
        KafkaSubscriberConfig tConfig = new KafkaSubscriberConfig();
        mSubscriber = new KafkaSubscriber( tConfig, this );

        listTopics();

        for( Pattern p : mSubscriptionTopics) {
            mSubscriber.addSubscriptionRegExp( p );
        }

        mSubscriber.start();

        while( true ) {
            try { Thread.sleep( 1000L ); }
            catch( InterruptedException e) {}
        }
    }


    private void listTopics() {
        Map<String, List<PartitionInfo>> tTopicMap = mSubscriber.listTopics();
        for( Map.Entry<String,List<PartitionInfo>> e : tTopicMap.entrySet() ) {
            System.out.println("topic: " + e.getKey());
            for( PartitionInfo pi : e.getValue()) {
                System.out.println("    " + pi.toString());
            }
        }
    }

    private void parseArguments(String[] args) {
        int i = 0;
        while (i < args.length) {
            if (args[i].compareToIgnoreCase("-bootServer") == 0) {
                mBootServer = args[i + 1];
                i++;
            }
            if (args[i].compareToIgnoreCase("-logMode") == 0) {
                mLogMode = LOG_MODE.valueOf(args[i + 1]);
                i++;
            }
            if (args[i].compareToIgnoreCase("-checkSeqNo") == 0) {
                mCheckSeqNo = Boolean.parseBoolean(args[i + 1]);
                i++;
            }
            if (args[i].compareToIgnoreCase("-autoCommit") == 0) {
                mAutoCommit = Boolean.parseBoolean(args[i + 1]);
                i++;
            }
            if (args[i].compareToIgnoreCase("-topics") == 0) {
                mSubscriptionTopics = parseTopics( args [i+1]);
                i++;
            }
            i++;
        }
    }

    private List<Pattern> parseTopics( String pTopicList ) {
        List<String> tTopicList = Arrays.asList( pTopicList.split(","));
        List<Pattern> tList = new ArrayList<>();
        tTopicList.forEach( t -> { tList.add( Pattern.compile( t ) );});
        return tList;
    }



    public void log( String pMsg ) {
        System.out.println( sdf.format(System.currentTimeMillis()) + " " + pMsg );
        System.out.flush();
    }
    @Override
    public void subscriberUpdate(ConsumerRecord<Long, String> pUpdate, Optional<JsonObject> jMsgHdr) {
        Topic tTopic = mTopics.get( pUpdate.topic() );
        if (tTopic == null) {
            tTopic = new Topic( pUpdate.topic(), pUpdate.key() );
            mTopics.put( pUpdate.topic(), tTopic );
        } else {
            tTopic.testAndSetSeqNo( pUpdate.key() );
        }


        tTopic.update( pUpdate.value() );
        mTotUpdates++;

        if (mLogMode == LOG_MODE.Normal) {
            log( formatNormal( pUpdate ));
        } else {
            log( formatVerbose( pUpdate, tTopic, jMsgHdr ));
        }

    }

    String formatNormal( ConsumerRecord<Long, String> pUpdate ) {
        return "Topic: " + pUpdate.topic() + " key: " + pUpdate.key() + " offset: " + pUpdate.offset() +
                " partition: " + pUpdate.partition() + " size: " +  pUpdate.value().length();
    }

    String formatVerbose( ConsumerRecord<Long, String> pUpdate, Topic pTopic, Optional<JsonObject> pMsgHdr  ) {
        String tMsgHdr = (pMsgHdr.isPresent()) ? ("\n   header: " + mPrettyPrinter.toJson( pMsgHdr.get()) + "\n") : "";


        int tSize = Math.min( pUpdate.value().length(), 70 );

        return  formatNormal( pUpdate )  + "\n   totupds: " + mTotUpdates + " topic: " + pTopic.mTopic +
                " updates: " + pTopic.mUpdates + " timestamp: " + sdf.format( pUpdate.timestamp() ) +
                " data: " + pUpdate.value().substring(0,tSize) + tMsgHdr;
    }



    @Override
    public void subscriberError(Throwable pException) {
        pException.printStackTrace();
        System.exit(0);
    }

     class Topic
    {
        private String  mTopic;
        private long    mSeqNo;
        public  long    mUpdates;
        public  long    mBytes;

        void update( String mData ) {
            mUpdates++;
            mBytes =+ mData.length();
        }

        Topic( String pTopic, Long pSeqNo ) {
            mTopic = pTopic;
            mSeqNo = (pSeqNo == null) ? 0 : pSeqNo.longValue();
        }


        void testAndSetSeqNo( Long pSeqNo ) {
            if (pSeqNo == null) {
                return;
            }
           if ((mSeqNo+1) != pSeqNo.longValue()) {
               log("topic: " + mTopic + " expected seqno (" + (mSeqNo+1) + " got (" + pSeqNo.longValue()  + ")");
            }
           mSeqNo = pSeqNo;
        }
    }
}
