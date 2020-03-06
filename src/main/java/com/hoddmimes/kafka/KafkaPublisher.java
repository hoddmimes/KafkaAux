package com.hoddmimes.kafka;



import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;

import java.util.Properties;
import java.util.concurrent.Future;


public class KafkaPublisher implements Callback
{
    private KafkaProducer<Long, byte[]>    mProducer;
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


    public Future<RecordMetadata> send(ProducerRecord<Long, byte[]> pDataRec) {
        return mProducer.send(pDataRec, this );
    }

    public Future<RecordMetadata> send(String pTopic, byte[] pMessage ) throws KafkaException
    {
        ProducerRecord<Long, byte[]> tDataRec = new ProducerRecord(pTopic, null, null, null, pMessage, null);
        return send( tDataRec );
    }

    public Future<RecordMetadata> send(String pTopic, Integer pPartition, byte[] pMessage ) throws KafkaException
    {
        ProducerRecord<Long, byte[]> tDataRec = new ProducerRecord(pTopic, pPartition, null, null, pMessage, null);
        return send( tDataRec );
    }

    public Future<RecordMetadata> send(String pTopic, Integer pPartition, Long pKey, byte[] pMessage ) throws KafkaException
    {
        ProducerRecord<Long, byte[]> tDataRec = new ProducerRecord(pTopic, pPartition, null, pKey, pMessage, null);
        return send( tDataRec );
    }

    public Future<RecordMetadata> send(String pTopic, Integer pPartition, Long pKey, byte[] pMessage, MsgHeader pHeader ) throws KafkaException
    {
        ProducerRecord<Long, byte[]> tDataRec = new ProducerRecord(pTopic, pPartition, null, pKey, pMessage, pHeader);
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
        System.out.println( "Topic: " + md.topic() +"Partition: " + md.partition() + " offset: " + md.offset());
    }
}
