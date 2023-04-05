package com.hoddmimes.kafka;



import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;


public class KafkaPublisher implements Callback
{
    private KafkaProducer<Long, String>    mProducer;
    private String mProducerId;

    public KafkaPublisher( String pProducerId, KafkaPublisherConfig pProperties ) throws KafkaException
    {
        mProducerId = pProducerId;
        pProperties.put("transactional.id", mProducerId );
        pProperties.put("enable.auto.commit", false);
        pProperties.put("isolation.level", "read_committed");
        mProducer = new KafkaProducer<>( pProperties );

        mProducer.initTransactions();
    }

    public KafkaPublisher( KafkaPublisherConfig pProperties ) throws KafkaException
    {
        mProducerId = null;
        mProducer = new KafkaProducer<>( pProperties );
    }


    public Future<RecordMetadata> send(ProducerRecord<Long, String> pDataRec) {
        return mProducer.send(pDataRec, this );
    }

    public Future<RecordMetadata> send(String pTopic, JsonObject pMessage ) throws KafkaException
    {
        return send( pTopic, null, null, pMessage, null);
    }

    public Future<RecordMetadata> send(String pTopic, Integer pPartition,  JsonObject pMessage ) throws KafkaException
    {
        return send( pTopic, pPartition, null, pMessage, null);
    }

    public Future<RecordMetadata> send(String pTopic, Integer pPartition,  JsonObject pMessage, JsonObject pMsgHeader  ) throws KafkaException
    {
        return send( pTopic, pPartition, null, pMessage, pMsgHeader);
    }

    public Future<RecordMetadata> send(String pTopic, Integer pPartition, Long pKey, JsonObject pMessage ) throws KafkaException
    {
        return send( pTopic, pPartition, null, pMessage, null);
    }


    public Future<RecordMetadata> send(String pTopic, Integer pPartition, Long pKey, JsonObject pMessage, JsonObject pMsgHeader ) throws KafkaException
    {
        List<MsgHeaderItem> tHdrList = null;
        if (pMsgHeader != null) {
            tHdrList = new ArrayList<>();
            tHdrList.add( new MsgHeaderItem( pMsgHeader ));
        }
        ProducerRecord<Long, String> tDataRec = new ProducerRecord(pTopic, pPartition, null, pKey, pMessage.toString(), tHdrList);
        return send( tDataRec );
    }




    public void startTx() throws KafkaException {
      mProducer.beginTransaction();
    }

    public void commit() throws KafkaException
    {
        mProducer.commitTransaction();
    }

    @Override
    public void onCompletion(RecordMetadata md, Exception exception) {
        if (exception != null) {
          exception.printStackTrace();
        }
        System.out.println( "Topic: \"" + md.topic() +"\" Partition: " + md.partition() + " offset: " + md.offset());
    }
}
