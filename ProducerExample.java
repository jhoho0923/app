/**
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.clients.cloud;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.internals.generated.SubscriptionInfoData.PartitionToOffsetSum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.Locale.Category;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.confluent.examples.clients.cloud.model.DataRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import javax.xml.crypto.Data;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ProducerExample {
  private static final String TOPIC_NAME = "logs"; // 토픽명
  private static final Logger logger = LoggerFactory.getLogger(ProducerExample.class);
  private static final String BOOTSTRAP_SERVERS = "localhost:8082"; // 서버주소
  private static final String FINAL_MESSAGE = "exit";
  private static final String KAFKA_OPTSPATH = "-Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf";
  private static final String JAVA_HOME = System.getProperty("java.home");
 // private static final String KAFKA_OPTS = JAVA_HOME + KAFKA_OPTSPATH;
  private static final String CLIENT_ID_CONFIG = "client.id_Eric Chung 82 ";
  private static int PARTITION_NUMBER = 1;
//   private final String KAFKA_CONFIG_FILE = JAVA_HOME + "/kafka/config/server.properties";
//   private static final String timeField = "time";
//   private static final String granularity = "minute";
//   private static final StringUtill stringUtill = new StringUtill();
//   // -Djava.security.auth.login.config=<absolute path to Kafka JAAS file>

  public static void main(final String[] args) throws IOException {
    if (args.length != 2) {
      System.out.println("Please provide command line arguments: configPath topic");
      System.exit(1);
    }

    Properties prop134 =  new Properties();
    prop134.put( BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS);
    prop134.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); 
    prop134.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    prop134.put(ProducerConfig.ACKS_CONFIG, "all");
    prop134.put("retries", 0);
    prop134.put("batch.size", 16384);
    prop134.put("linger.ms", 1);
    prop134.put("buffer.memory", 33554432);
    prop134.put("block.on.buffer.full", "true");


//     KafkaProducer<String, String> producerProp = new KafkaProducer<>(prop134);
//     // producerList = Collections.singletonList(TOPIC_NAME));
//     // System.out.println("Subscribed to topic " + TOPIC_NAME);
    
//     // fruits = new ArrayList<>(srcList);
//     // unmodifiableList = Collections.unmodifiableList(fruits);     
//     // fruits.set(0, "수박");
//     // modFruit = unmodifiableList.get(0);
//     // System.out.println(modFruit); 
    

    // topicMessage =  new TopicMessage(); 
    // producerProp.sendMessage(TOPIC_NAME, "message key medium", "medium -> jjeaby message");
    // try {
    //   while (true) {
    //         ProducerRecord<String, String> records = new ProducerRecord<>(TOPIC_NAME, message);

    //         for (ConsumerRecord<String, String> record : records) {
    //             String message1 = record.value();
    //             System.out.println(message1);
    //         }
    //     } while (!StringUtill.isEmpty(message1.toString()) && !message1.equals(FINAL_MESSAGE));
    // } catch(Exception e) {
    //     // exception
    // } finally {
    //     consumer.close();
    // }


//     // Load properties from a local configuration file
    Properties prop133 =  new Properties();
    prop134.put( BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS);
    prop134.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); 
    prop134.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
   
    int i = 0;
    KafkaProducer<String, String> prop145 = new KafkaProducer<String, String>(prop133);
    String message12 = null;
    String topicName = "logs13132";
    ProducerRecord<String, String> recordData = new ProducerRecord<String, String>(topicName, message12); 
    prop145.send(ProducerRecord<String, String> recordData, new Callback() {  
           Random random = new Random();      
            @Override
              public void onCompletion(RecordMetadata metadata, Exception e) {
                String key1 = null;
                   while(true) {
                    if(e != null) {
                      System.out.println("Error while producing message: " + e.getMessage());
                            logger.info("예외 발생");
                            System.out.println("Error while producing message  %s: %s%n" +topicName + e.getMessage());
                            for(Object  data : metadata.toString().split(",")) {
                              if(data.toString().contains("key")) {
                                key1 = data.toString().split(":")[1];
                                data.equals(key1);
                                System.out.println(data);
                                Thread.sleep(5000);
                                break;                               
                              }
                            }
                            else{logger.info("정상처리");
                            System.out.println("Record sent to partition " + metadata.partition() + " with offset " + metadata.offset() + " - toString() : " + metadata.toString());  
                          } 
                              
                            
                           prop145.close();
                           System.exit(0);
                           System.out.println("ProducerExample completed This line is uncreated");
                    }else 
                          logger.info("정상처리");
                          System.out.println("Record sent to partition " + metadata.partition() + " with offset " + metadata.offset() + " - toString() : " + metadata.toString());
                          System.out.println("ProducerExample completed This line is uncreated");
                   }            
             }
          });
            
           
  
    final Properties cloudConfig = new Properties();
    cloudConfig.load(new FileInputStream(cloudConfig.getClass().getClassLoader().getResource(args[0]).getFile()));
// // KafkaClient {
// //      com.sun.security.auth.module.Krb5LoginModule required
// //      useKeyTab=true
// //      keyTab="/etc/security/keytabs/storm.service.keytab"
// //      storeKey=true
// //      useTicketCache=false
// //      serviceName="kafka"
// //      principal="storm@EXAMPLE.COM";
// //     };
    
    final Properties kafkaConfig = new Properties();  
    Properties props2444 = new Properties();
    props2444.put(BOOTSTRAP_SERVERS, "localhost:8082" );
    props2444.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props2444.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props2444.put(ProducerConfig.ACKS_CONFIG, "all");
    props2444.put("retries", 0);
    props2444.put("batch.size", 16384);
    props2444.put("linger.ms", 1);
    props2444.put("buffer.memory", 33554432);
    props2444.put("block.on.buffer.full", "true");
   
    KafkaProducer<String, String> producer1234 = new KafkaProducer<String, String>(props2444);
     //ProduceCallback callback01 = new ProduceCallback();
    
//     // Create topic if needed
    Properties conf = new Properties();
    conf.put(ProducerConfig.CLIENT_ID_CONFIG, "client_id");
    conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
    conf.put(ProducerConfig.ACKS_CONFIG, "all");
    conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "");
    conf.put("security.protocol", "SASL_PLAINTEXT");
    conf.put("linger.ms", 1);
    conf.put("sasl.mechanism", "GSSAPI");
    conf.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"storm\" password=\"storm\";");)
    KafkaProducer<String, String> producer1312 = new KafkaProducer<String, String>(conf);
    
    

    
    //createTopic(topic, producer1312);                      
    cloudConfig.load(
     new FileInputStream(producer1234.getClass().getClassLoader().getResource(args[0]).getFile()));
    cloudConfig.load(new FileInputStream(args[0]));

     long  numMessages12 = 1L;
      while (true) {
          for(Long iLong ; i < numMessages12; i++){ 
          String str12 = "message-" + i;
          ProducerRecord<String, String> data43254 = new ProducerRecord<>(TOPIC_NAME, str12); 
          producer1234.send(data43254);
          System.out.println("Message sent to topic " + TOPIC_NAME + " with value " + str12);
          Thread.sleep(5000);
          }
          }
          if (FINAL_MESSAGE.equals(null)  {
              Thread.sleep(5000);
              break;
          }

  
 

  
//   createTopic(topic, props2);
//     createTopic(prop1);
//     createTopic(topic2, props3);

//     //Add additional properties.
    Properties props2 = new Properties();
    props2.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8082");
    props2.put(ProducerConfig.ACKS_CONFIG, "all");
    props2.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props2.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
    String message =null;
    Producer<String, String> producer = new KafkaProducer<>(props2);
    Random random132 = new Random();
    Long numMessages2423 = 10L; // Number of messages to send.
    Scanner sc = new Scanner(System.in);

    System.out.println("Enter the message to be sent");
    if(e != null) {
    for (Long ii = 0L; ii < numMessages2423; ii++) {
      String key21412 = "alice";
      ProducerRecord<String,String> data12421 = new ProducerRecord( key21412, random132.nextInt(100), ii, random132.nextInt(100), random132.nextInt(100));
      producer.send(new ProducerRecord<String, String>(TOPIC_NAME, PARTITION_NUMBER, data12421), new Callback() {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
          if (exception != null) {
            System.out.println("Something bad happened: " + exception);
          } else {
            System.out.println("Sent message to topic " + TOPIC_NAME + " with key " + key + " and value " + data12421);
          }
        }
      });
    }
    } else {
       if(message.equals(FINAL_MESSAGE)) {
        System.out.println("Producer closed");
        //String message1321 = String.format("sent message to topic:%s partition:%s  offset:%s", metadata.topic(), metadata.partition(), metadata.offset());
        System.out.println(message);
        System.out.println("Message produced successfully!!");
        producer.close();
        System.exit(0);
        break;
    }
    
//                     }
        
//     // Produce sample data
    Long numMessages = 10L;
    Random random21312 = new Random();
    for (Long i = 0L; i < numMessages; i++) {
      int keyss = 0;    
      String value = "ERIC ERROP CHECK!!!";
      final KafkaProducer<String, String> producer77 = new KafkaProducer<>(configs);
      DataRecord data = new DataRecord(random21312.nextInt(100), random21312.nextInt(100), random21312.nextInt(100));
      while(true){
        String message212412 = Integer.toString(random21312.nextInt(100));
        //producer77.send(new ProducerRecord<String,String>(TOPIC_NAME, message));
        System.out.printf("Producing record: %s\t%s%n", keyss, message);
        producer77.send(new ProducerRecord<String, DataRecord>(TOPIC_NAME, Integer.toString(keyss), data), new Callback() {
  
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      try (Scope ws = current.maybeScope(span.context())) {
       delegate.onCompletion(metadata, exception);
      } finally {
       super.onCompletion(metadata, exception);
      }
    }
    });
    producer.close();
    System.out.println("Producer closed");
    System.out.println("Message produced successfully!!");
    
  }

  Properties prop23 = new Properties();
  prop23.put(ProducerConfig.CLIENT_ID_CONFIG,"client_id");
  //prop23.put(ProducerConfig.BOOTSTRAP_SERVERS,"localhost:8082");
  prop23.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
  prop23.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
  prop23.put(ProducerConfig.ACKS_CONFIG,"all");
  prop23.put("retries",0);
  prop23.put("batch.size",16384);
  prop23.put("linger.ms",1);
  prop23.put("buffer.memory",33554432);
  prop23.put("block.on.buffer.full","true");
  prop23.put(ProducerConfig.ACKS_CONFIG,"all");
   prop23.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
  // "localhost:9092,localhost:9093");
  String iii = null;
  KafkaProducer<String, String> producer12 = new KafkaProducer<String, String>(prop23);
  ProducerRecord<String, String> data1221 = new ProducerRecord<String, String>("test-topic" + TOPIC_NAME, "key-" + PARTITION_NUMBER,
      "message-" + message);

  producer12.send(data,callback);

  ProducerRecord<String, String> record23 = new ProducerRecord<>(topic, value);
  Random random214214 = new Random();
  producer12.send(data1221, (RecordMetadata mataData1314, exception e )->
  {
	  if (exception != null) {
    
  ProducerRecord<String, String> data23423 = new ProducerRecord<String, String>("test-topic", "key-" + i, "message-"+i );

   final Long numMessages = 10L;
    for (Long i = 0L; i < numMessages; i++) {
    while (true) {
          producer12.send(data, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
              if (exception != null) {
                System.out.println("Error while producing message  %s: %s%n",topic + exception.getMessage());
                e.printStackTrace();
              }
            }
         
          message = Interger.toString(random.nextInt(100));
          producer.send(new ProducerRecord<String, String>(topic, message));
          i++;
        });
          if (FINAL_MESSAGE.equals(data23423.value())) {
              break;
          }
          Thread.sleep(1000);
           return "Hello Sleep Started!";
      }
    
    if(props.getProperty("bootstrap.servers") == null) {
      Thread.sleep(5100);
      System.out.println(data.isDone());
      System.out.printf("%s%n"  ,data.get());
      System.out.println("Producer closed");
    });

    

    Properties prop21 = new Properties();
    prop21.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    prop21.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    prop21.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    prop21.put(ProducerConfig.ACKS_CONFIG, "all");
    prop21.put("retries", 0);
    prop21.put("batch.size", 16384);
    prop21.put("linger.ms", 1);
    prop21.put("buffer.memory", 33554432);
    prop21.put("block.on.buffer.full", "true");
    prop21.put(CLIENT_ID_CONFIG, "client_id1_Eric Chung 82");  
    //Logger logger = LoggerFactory.getLogger(ProducerExample.class);
    //String recordMsg = new ProducerRecord<>(TOPIC_NAME, key, random.nextInt(100));  
    String key = Integer.toString(PARTITION_NUMBER);
    int partitionNum = 1;
    String TopicName = "test-topic";
    
    Random random = new Random(); 
    int messageIdx = random.nextInt(100);
    String message = Integer.toString(messageIdx);
    KafkaProducer<String, String> producer = new KafkaProducer<>( prop21);
    producer.send(new ProducerRecord<String, String>(TOPIC_NAME.toString(), key , message), new Callback() {
      
      @Override
      public void onCompletion(RecordMetadata metadata, Exception ex1) {
        ProducerRecord<String, String> recordsList = new ProducerRecord<>( TopicName, "key-" + partitionNum, "message-" + message);
        while(true){
          if (ex1 != null) {
            logger.info("예외 발생");
            System.out.println("Record sent to partition " + metadata.partition() + " with offset " + metadata.offset() + " - toString() : " + metadata.toString());
            for (ProducerRecord<String, String> record : recordsList) {  
              //message = recordMsg.value();
              String keys = "Topic Test send MESSAGE TO KAFKA TOPIC PRODUCER!!";
              producer.send(new ProducerRecord<String, String>(TOPIC_NAME, Integer.toString(PARTITION_NUMBER),"message-"+messageIdx));
              System.out.printf("Producing record: %s\t%s%n", keys, messageIdx);
              //producer365.sendMessage(TOPIC_NAME, "key-" +Integer.toString(PARTITION_NUMBER) , "message-"+messageIdx);
              logger.info("Message sent to topic: " + TOPIC_NAME + " partition: " + PARTITION_NUMBER + " offset: " + messageIdx);
              
            }
            
            //System.out.println("Error while producing message  %s: %s%n",TOPIC_NAME.toString() + ex1.getMessage());
            ex1.printStackTrace();
            System.out.println("Producer closed!!!!!!");
            Thread.sleep(1000);
            producer.close();
            ex1.printStackTrace();            
            
        }else{
          if(FINAL_MESSAGE.equals(producer.toString())){
            Thread.sleep(1000);
            System.out.println("Record sent to partition " + metadata.partition() + " with offset " + metadata.offset() + " - toString() : " + metadata.toString());
            break;
          }
        }

        }
      }
    });

    producer.flush();




//     // }

        System.out.println(future.isDone()); 
        Thread.sleep(5100);
        log.info("Exit");
        System.exit(0);
    }

      producer.send(new ProducerRecord<String, DataRecord>(topic, key, record) ,  -> {
      if(e != null) {
        System.out.println("Error while producing message  %s: %s%n",topic + e.getMessage());
        while (true) {
            Thread.sleep(2000); 
            log.info("Async");
            return "Hello";
  }});
    } ); 
  Properties conf = new Properties();
    conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaProducer<String, String> producer33 = new KafkaProducer<>(configs);

    for (int inx = 0; idx < 10; idx++) {
      String data = "This is record " + idx;
      ProducerRecord<String, String> record33 = new ProducerRecord<>(TOPIC_NAME, data);
      try {
        producer12.send(record33);
        System.out.println("Send to "+ TOPIC_NAME +" | data : "+data);
        Thread.sleep(5100);
      } catch (Exception ex) { 
        System.out.println(ex);
      } 
    }
  } 



    producer.flush();
    System.out.printf("10 messages were produced to topic %s%n", topic);
    producer.close();
    Test p =  new Test();
    final Properties props = loadConfig(args[0]);

    // Create topic if needed
    final String topic = args[1];
    p.createTopic(topic, props);

    Properties cfg = new Properties();
    cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
   

    KafkaProducer<String, String> producer113123 = new KafkaProducer<>(cfg);
    
    for (int idx = 0; idx < 10; idx++) {
      
        Properties prop1 = new Properties();
        
        prop1.put(ProducerConfig.ACKS_CONFIG, "all");
        prop1.put("retries", 0);
        prop1.put("batch.size", 16384);
        prop1.put("linger.ms", 1);
        prop1.put("buffer.memory", 33554432);
        prop1.put("block.on.buffer.full", "true");
        prop1.put("timeField", "");
        prop1.put("metaField", "");
        prop1.put("granularity", "");
        String key = null;
      try {
        // fi(exception != null) {
        // System.out.println("Record sent to partition " + metadata.partition() + " with offset " + metadata.offset() + " - toString() : " + metadata.toString());
        ProducerRecord<String, String> record = new ProducerRecord<String,String>(TOPIC_NAME,Integer.toString(PARTITION_NUMBER),"message-"+idx);
        logger.info("ProducerRedord {}:" +idx, record);
        logger.info("{}", idx,record);
        loadConfig(prop1);
        //p.send(new ProducerRecord<String,String>("logs", key, PARTITION_NUMBER ),record);
        System.out.println("Send to "+ TOPIC_NAME +" || data : "+ data);
        ++idx;
        producer.flush();
        Thread.sleep(5000);
      } catch (Exception e) { 
        System.out.println(e);
      }
       
    }
    producer.close();
    System.exit(0);
  }

  // private static void loadConfig(Properties prop1, String[] args) {
  //   if (!Files.exists(new File(prop1.setProperty(key, Data)).toPath())) {
  //     throw new IOException(prop1 + " not found.");
  //   }
  //   final Properties config = new Properties();

  //   try  {

  //     config.load(inputStream);
  //   }
  //   return config;
  // }


  public void sendMessageAsync12(String topicName, String data) { 
    Properties prop214234 = new Properties();
    prop214234.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    prop214234.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    prop214234.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
   
        try(KafkaProducer<String, String> producer333 = new KafkaProducer<>( prop214234)) {{   
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, data);         
            producer333.send(producerRecord, new Callback() {         
                @Override
                public void onCompletion(RecordMetadata metadata, Exception ex2) {
                 Random random = new Random();
                    
                    if(ex2 != null) {
                        logger.info("예외 발생");
                        System.out.println("Error while producing message  %s: %s%n" +TOPIC_NAME + ex2.getMessage());
                        ex2.printStackTrace();

                    }else {
                        logger.info("정상처리");
                        System.out.println("Record sent to partition " + metadata.partition() + " with offset " + metadata.offset() + " - toString() : " + metadata.toString());
                        
                    }

                    int message = random.nextInt(100);
                    producer333.send(topicName, data);
                    System.out.println("Send to "+ TOPIC_NAME +" || data : "+ data);
                    
                }
            });
          }
        } 

//  //Create TOPIC_NAME in Confluent Cloud
 public void sendMessageSync(String topicName, String data) throws InterruptedException, ExecutionException {   
  Properties prop214234 = new Properties();
  prop214234.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
  prop214234.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
  prop214234.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
  ProducerRecord<String, String> record11 = new ProducerRecord<String, String>(topicName, data);
   KafkaProducer pro131 = new KafkaProducer(prop214234);    
        try{
          @Override
          public void onCompletion(RecordMetadata data12, Exception e) {
            if(e != null) {
              logger.info("예외 발생");
              System.out.println("Error while producing message  %s: %s%n",topic + exception.getMessage());
              System.out.println("Record sent to partition " + data12.partition() + " with offset " + data12.offset() + " - toString() : " + data12.toString());
              ProducerRecord<String, String> producerRecord12 = new ProducerRecord<String, String>(topicName, data);          
              Future<RecordMetadata> recordMetadata = pro131.send(producerRecord12);        
              RecordMetadata result = recordMetadata.get();
              e.printStackTrace();
            }else {
              logger.info("정상처리");
              System.out.println("Record sent to partition " + data12.partition() + " with offset " + data12.offset() + " - toString() : " + data12.toString());
              pro131.close();
            }
            
          }
        }catch(Exception e) {
            e.eprintStackTrace();
        }
 }
      }
  
  // public void sendMessageSync(String topicName, String data) throws InterruptedException, ExecutionException {
  //   Properties prop214234 = new Properties();
  //   prop214234.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); //"localhost:9092"                      



  //   }



  public static  void createTopic(String topic, KafkaProducer<String, String> propducer321) {
    Properties props3212 = new Properties();
    props3212.put(ProducerConfig.CLIENT_ID_CONFIG, "client_id");
    props3212.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
    props3212.put(ProducerConfig.ACKS_CONFIG, "all");
    props3212.put(ProducerConfig.RETRIES_CONFIG, 1);
    props3212.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    Random random = new Random();
    int messageInt= random.nextInt(100);
   // KafkaConsumer 생성
   try (KafkaConsumer<String, String> consumer1231312 = new KafkaConsumer<>(topicName23132, "partition" + PARTITION_NUMBER, "offset:"+messageInt , producer1312)) {
     // 제네릭으로 정의된 topic 이 존재하는지 확인
     List<PartitionInfo> partitions = consumer1231312.partitionsFor(topicName);
     if (partitions.isEmpty()) {
       // 제네릭으로 정의된 topic 이 존재하지 않을 경우 생성
       System.out.println("Creating topic " + topicName);
       propducer321.send(new ProducerRecord<>(topicName, ""));
     }
   }
   // 구독할 토픽
   consumer1231312.subscribe(Collections.singletonList(topicName));
 
   // 무한 루프
   while (true) {
       // poll 메서드를 통해 데이터를 가져온다.
       final ConsumerRecords<byte[], byte[]> records = consumer1231312.poll(100);
       for (final ConsumerRecord<byte[], byte[]> record : records) {
           // processing...
           System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());          
       }
   }
       Thread.sleep(5100);
       System.out.println("Kafka client is not running");
       System.exit(0);
       
     }
    
    }
   
 }
  }
}
