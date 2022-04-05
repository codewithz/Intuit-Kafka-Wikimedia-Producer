package com.intuit.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServer="127.0.0.1:9092";

        //Create Producer Properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create a Producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        String topic="wikimedia.changes";
        String url="https://stream.wikimedia.org/v2/stream/recentchange";

        EventHandler eventHandler=new WikimediaChangeHandler(producer,topic);

        EventSource.Builder builder=new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource=builder.build();

        //Start the producer-- in different thread

        eventSource.start();

        // We produce for 5 mins and block the program until then
        TimeUnit.MINUTES.sleep(5);
    }
}
