package com.hoddmimes.kafka;



import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;

import java.util.Properties;
import java.util.concurrent.Future;


public class KafkaPublisher
{
    KafkaProducer<Long, byte[]>    mProducer;

    public KafkaPublisher( KafkaPublisherConfig pProperties ) throws KafkaException
    {
        mProducer = new KafkaProducer<>( pProperties );
    }

    public Future<RecordMetadata> send(String pTopic, Integer pPartition, Long pKey, byte[] pMessage, Iterable<Header> pHeaders) {
        ProducerRecord<Long, byte[]> tDataRec = new ProducerRecord(pTopic, pPartition, null, pKey, pMessage, pHeaders);
        return mProducer.send(tDataRec);
    }

    public Future<RecordMetadata> send(String pTopic, byte[] pMessage ) throws KafkaException
    {
        return send( pTopic, null, null, pMessage, null);
    }

    public Future<RecordMetadata> send(String pTopic, Integer pPartition, byte[] pMessage ) throws KafkaException
    {
        return send( pTopic, pPartition, null, pMessage, null);
    }

    public Future<RecordMetadata> send(String pTopic, Integer pPartition, Long pKey, byte[] pMessage ) throws KafkaException
    {
        return send( pTopic, pPartition, pKey, pMessage, null);
    }

}
