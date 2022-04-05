package com.intuit.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {
    KafkaProducer<String,String> producer;
    String topic;
    private Logger logger= LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    WikimediaChangeHandler(KafkaProducer<String,String> producer,String topic){
        this.producer=producer;
        this.topic=topic;
    }

    @Override
    public void onOpen() throws Exception {
        // No Action Here
    }

    @Override
    public void onClosed() throws Exception {
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        producer.send(new ProducerRecord<>(topic,messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {
        // No action here
    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("Error in stream reading",throwable);
    }
}
