package com.hoddmimes.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaAux
{
    private Properties  mProperties;
    private AdminClient mAdminClient;

    public KafkaAux() {
        this( "localhost", 9092);
    }

    public KafkaAux( String pBootServer, int pBootServerPort ) {
      mProperties = getProperties( pBootServer, pBootServerPort );
      mAdminClient = AdminClient.create(mProperties);
    }


    private Properties getProperties( String pBootServer, int pBootServerPort ) {
        Properties p  = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, pBootServer + ":" + String.valueOf( pBootServerPort));
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
        return p;
    }


    public List<String> listTopics() throws KafkaException {
        return listTopics( false );
    }

    public List<String> listTopics(boolean pListAll) throws KafkaException {
        ListTopicsResult listResult = mAdminClient.listTopics();
        KafkaFuture<Map<String,TopicListing>>  resultFuture = listResult.namesToListings();
        List<String> tTopics = new ArrayList<>();

        Map<String,TopicListing> tTopicMap = null;
        try { tTopicMap = resultFuture.get(); }
        catch( ExecutionException ee ) {}
        catch( InterruptedException ie ) {}

        for( Map.Entry<String,TopicListing>  tEntry :  tTopicMap.entrySet()) {
            if ((!pListAll) && (!tEntry.getValue().isInternal())) {
                tTopics.add(tEntry.getKey());
            } else {
                tTopics.add(tEntry.getKey());
            }
        }
        return tTopics;
    }



    public boolean topicExists( String pTopic  ) throws KafkaException {
        TopicCollection topicCollection = TopicCollection.ofTopicNames( Arrays.asList(pTopic));
        DescribeTopicsResult topicResult = mAdminClient.describeTopics( topicCollection );
        KafkaFuture<Map<String, TopicDescription>>  resultFuture = topicResult.allTopicNames();

        try {
            resultFuture.get();
        }
        catch( ExecutionException ee ) {}
        catch( InterruptedException ie ) {}
        catch (UnknownTopicOrPartitionException ue) {
            return true;
        }
        return false;
    }

    public boolean createTopic( String pTopic ) throws KafkaException {
        return createTopic(pTopic, 1, 1);
    }

    public boolean createTopic( String pTopic, int pNumPartitions, int pReplications ) throws KafkaException {
        NewTopic newTopic = new NewTopic(pTopic, pNumPartitions, (short) pReplications);
        CreateTopicsResult topicResult = mAdminClient.createTopics(Arrays.asList(newTopic));
        KafkaFuture<Void> resultFuture = topicResult.all();
        try {
            resultFuture.get();
        }
        catch( ExecutionException ee ) {}
        catch( InterruptedException ie ) {}
        catch (TopicExistsException ue) {
            return false;
        }
        return true;
    }

    public boolean deleteTopic( String pTopic ) throws KafkaException {
        TopicCollection tTopicCollection = TopicCollection.ofTopicNames( Arrays.asList( pTopic ));
        DeleteTopicsResult topicResult = mAdminClient.deleteTopics( tTopicCollection );
        KafkaFuture<Void> deleteFuture = topicResult.all();
        try {
            deleteFuture.get();
        }
        catch( ExecutionException ee ) { return false;}
        catch( InterruptedException ie ) { return false;}
        catch( UnknownTopicOrPartitionException ue) { return false; }
        return true;
    }

    public void deleteAllTopic() throws KafkaException {
        List<String> tTopics = listTopics(false);
        DeleteTopicsResult topicResult = mAdminClient.deleteTopics( tTopics );
        KafkaFuture<Void> deleteFuture = topicResult.all();
        try {
            deleteFuture.get();
        }
        catch( ExecutionException ee ) {}
        catch( InterruptedException ie ) {}
    }


    public void close() {
        mAdminClient.close();
    }


}
