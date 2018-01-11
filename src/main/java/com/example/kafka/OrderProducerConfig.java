package com.example.kafka;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import com.example.kafka.util.OrderSerde;


public class OrderProducerConfig {

	public static Properties createConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-order-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        
        OrderSerde orderSerde = new OrderSerde();
        
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, orderSerde.getClass());
        return props;
    }

}
