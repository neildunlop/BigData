package com.datinko.prototype.bigdata.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Properties;

/**
 * Kafka Producer that reads files from the file system and dispatches messages to a target Kafka topic.
 */
public class KafkaProducer implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(KafkaProducer.class);
    private final Charset ENCODING = Charset.forName("UTF-8");

    private String topic = "";
    private InputStream inputStream = null;

    private ProducerConfig producerConfig = null;
    private Producer<Integer, String> producer = null;


    public KafkaProducer(String topic, InputStream stream)
    {
        this.topic = topic;
        this.inputStream = stream;
    }

    protected void configure() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "async");

        producerConfig = new ProducerConfig(props);
    }

    public void run() {

        configure();

        BufferedReader reader = null;

        try {
            try {
                reader = new BufferedReader(
                        new InputStreamReader(this.inputStream, ENCODING));
                String line = null;
                LOGGER.debug("Producing messages");
                producer = new Producer<Integer, String>(producerConfig);
                while ((line = reader.readLine()) != null) {
                    producer.send(new KeyedMessage<Integer, String>(
                            this.topic, line));
                }
                LOGGER.debug("Done sending messages");
            } catch (IOException ex) {
                LOGGER.fatal("IO Error while producing messages", ex);
                LOGGER.trace(null, ex);
            }
        } catch (Exception ex) {
            LOGGER.fatal("Error while producing messages", ex);
            LOGGER.trace(null, ex);
        } finally {
            try {
                if (reader != null) reader.close();
                if (producer != null) producer.close();
            } catch (IOException ex) {
                LOGGER.fatal("IO error while cleaning up", ex);
                LOGGER.trace(null, ex);
            }
        }
    }


}
