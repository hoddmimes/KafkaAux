package com.hoddmimes.kafka;



import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;

import java.util.Properties;
import java.util.concurrent.Future;


public class KafkaPublisher
{
    KafkaProducer<String, byte[]>    mProducer;

    public static Properties getDefaultProperties( String pBootstrapServerPort) {
        Properties p = new Properties();

        p.put("bootstrap.servers", pBootstrapServerPort);
        p.put("acks", "all");
        p.put("retries", 0);
        p.put("batch.size", 16384);
        p.put("linger.ms", 1);
        p.put("client.id","");
        p.put("buffer.memory", 33554432);
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return p;
    }


    public KafkaPublisher( Properties pProperties ) throws KafkaException
    {
        mProducer = new KafkaProducer<>( pProperties );
    }

    public Future<RecordMetadata> send(String pTopic, byte[] pMessage, int pPartition, Long pKey  ) throws KafkaException
    {
        ProducerRecord<String, byte[]> tDataRec = new ProducerRecord<>( pTopic,  pPartition, (pKey == null) ? null : String.valueOf( pKey ), pMessage );
        return mProducer.send( tDataRec );
    }

}
