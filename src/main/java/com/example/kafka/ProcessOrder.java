package com.example.kafka;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.kafka.domain.Order;
import com.example.kafka.util.OrderSerde;

public class ProcessOrder {
	
	private final static Logger log = LoggerFactory.getLogger(ProcessOrder.class);

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws InterruptedException {
		Properties config = OrderProducerConfig.createConfig();
		final StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, Order> source = builder.stream("freshorders");
        
        OrderSerde orderSerde = new OrderSerde();
        
        // Modify the order's status as 'Confirmed' and push them in the output queue
        KStream<Long, Order> confirmedOrdersStream = source.map((key, value)-> {
        		value.setStatus("CONFIRMED"); 	
        		System.out.println("Key: "+key.getClass()+", Value: "+value.getClass()+", Fetched order: "+value.toString());
        		return KeyValue.pair(key, value);
        	})
        .through("confirmedorders", Produced.with(Serdes.Long(), orderSerde));
        
        // Find the total sales so far and save them inside a queue
        KStream<Long, Double> kStream = confirmedOrdersStream.mapValues(value -> value.getPrice());
        
        KGroupedStream<Integer, Double> kGroupedStream = kStream.groupBy((key, value) -> 1, Serdes.Integer(), Serdes.Double());
        
        KTable<Integer, Double> kt = kGroupedStream.reduce((v1, v2) -> v1 + v2);
        
        kt.toStream().to("totalsales", Produced.with(Serdes.Integer(), Serdes.Double()));
        
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
        } finally {
            System.exit(0);
        }
	}

}
