package com.example.kafka;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import com.example.kafka.domain.Order;

public class ProcessOrder {
	
	private static String INPUT_ORDER = "freshorders";
	
	private static String CONFIRMED_ORDER = "confirmedorders";
	
	private static String ZIPPED_ORDERS = "zippedorders";
	
	private static String TOTAL_SALES = "totalsales";
	

	public static void main(String[] args) {
		Properties config = OrderProducerConfig.createConfig();
		final StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, Order> source = builder.stream(INPUT_ORDER);
        
        // Modify the order's status as 'Confirmed' and push them in the output queue
        KStream<Long, Order> confirmedOrdersStream = source.map((key, value)-> {
        		value.setStatus("CONFIRMED"); 	
        		return KeyValue.pair(key, value);
        	})
        .through(CONFIRMED_ORDER);
        
        // Find the total sales so far and save them inside a queue
        confirmedOrdersStream
        		.mapValues(value -> value.getPrice())
        		.groupBy((key, value) -> 1)
        		.reduce((v1, v2) -> v1 + v2)
        		.toStream()
        		.to(TOTAL_SALES);
        
        
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, config);
        final CountDownLatch latch = new CountDownLatch(1);
        
        // attach shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
	        	streams.close();
	        latch.countDown();
        }));
 
        try {
            streams.start(); // Start the Stream processor
            latch.await();   // When countdownLatch trips, stream processing stops
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
	}

}
