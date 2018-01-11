package com.example.kafka.util;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.example.kafka.domain.Order;

public class OrderSerde implements Serde<Order> {
    private OrderSerializer serializer = new OrderSerializer();
    private OrderDeserializer deserializer = new OrderDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<Order> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Order> deserializer() {
        return deserializer;
    }
}