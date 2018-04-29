package com.mkjoin.function;

import java.util.Iterator;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import com.mkjoin.kafka.KafkaSink;
import com.mkjoin.metadeta.MetaDataLoader;

public class KafkaProducerVoidFunction implements VoidFunction<Iterator<String>> {
	
	private static final long serialVersionUID = 1223462981096254502L;
	
	private final Broadcast<KafkaSink> broadCast;

	public KafkaProducerVoidFunction(JavaSparkContext sc, String topicName) {
		KafkaSink kafkaSink = new KafkaSink(MetaDataLoader.getKafkaProperties(), topicName);
		broadCast = sc.broadcast(kafkaSink);
	}

	public void call(Iterator<String> joinedData) throws Exception {
		final KafkaSink kafkaSink = broadCast.value(); 
		while(joinedData.hasNext()) {
			kafkaSink.sendMessage(joinedData.next());
		}
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				kafkaSink.close();
			}
		}); 
	}
}
