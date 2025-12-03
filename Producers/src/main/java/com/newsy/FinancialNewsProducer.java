package com.newsy;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.rometools.rome.io.*;
import com.rometools.rome.feed.synd.*;

import org.json.JSONObject;

import java.net.URL;
import java.util.Properties;

public class FinancialNewsProducer {

    private static final String TOPIC = "finance-news";
    private static final String RSS_URL = "https://www.livemint.com/rss/markets"; // or CNBC, Reuters etc.

    public static void main(String[] args) throws Exception {

        // Kafka producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Parse RSS feed
        URL feedUrl = new URL(RSS_URL);
        SyndFeedInput input = new SyndFeedInput();
        SyndFeed feed = input.build(new XmlReader(feedUrl));

        System.out.println("Sending articles from: " + RSS_URL);

        for (SyndEntry entry : feed.getEntries()) {

            JSONObject json = new JSONObject();
            json.put("title", entry.getTitle());
            json.put("link", entry.getLink());
            json.put("published", entry.getPublishedDate() + "");
            json.put("source", "LiveMint");

            String message = json.toString();

            producer.send(new ProducerRecord<>(TOPIC, entry.getTitle(), message));

            System.out.println("Published: " + entry.getTitle());
        }

        producer.flush();
        producer.close();
    }
}
