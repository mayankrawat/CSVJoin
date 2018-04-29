package com.mkjoin.kafka;

import java.io.Serializable;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSink extends KafkaProducer<String, String> implements Serializable {

	private static final long serialVersionUID = -6397147999772264135L;
	
	private String topic;
	
	public KafkaSink(Properties properties, String topic) {
		super(properties);
		this.topic = topic;
	}
	
	public boolean sendMessage(String value) {
		return send(new ProducerRecord<String, String>(topic, value)).isDone();
	}
}