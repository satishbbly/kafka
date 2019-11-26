package com.satish.java.KafkaDemo;

//import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class NewSupplierConsumer{

    public static void main(String[] args) throws Exception{

        String topicName = "test";
        String groupName = "SupplierTopicGroup";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("group.id", groupName);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

   //     InputStream input = null;
        KafkaConsumer<String, String> consumer = null;

        try {
			/*
			 * input = new FileInputStream("consumer.properties"); props.load(input);
			 */
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topicName));
            Duration timeout =Duration.ofSeconds(100);
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(timeout);
                for (ConsumerRecord<String, String> record : records){
                    System.out.println("Supplier id= " + String.valueOf(record.value()) + " Supplier  Name = " + record.topic() + " Supplier Start Date = " + record.timestamp());
                }
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
           // input.close();
            consumer.close();
        }
    }
}