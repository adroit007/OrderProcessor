package com.example.kafka.util;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.example.kafka.domain.Order;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OrderDeserializer implements Deserializer<Order> {

	@Override
	public void close() {
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
	}

	@Override
	public Order deserialize(String arg0, byte[] arg1) {
		ObjectMapper mapper = new ObjectMapper();
		Order req = null;
		try {
			req = mapper.readValue(arg1, Order.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return req;
	}

	public OrderDeserializer() {
		// TODO Auto-generated constructor stub
	}
	
	

}