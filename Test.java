import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;

public  class Test  { 
  
  private static int PARTITION_NUMBER_1= 1;
  private static Admin aadminClient;
  private static int NUM_PARTITIONS = 1; 
  // 토픽 스토리지에 저장된 메시지를 스트림을 읽어오는 수행문
  
  
  public static Properties createTopic(String topic, Properties prop111) {
     
    //새로운 인스턴스 토픽 오브젝트를 선언한다
    //final NewTopic newTopic = new NewTopic(Test.c(topic, prop111));
    
    ObjectMapper objectMapper = new ObjectMapper((new ObjectMapper()).getFactory());
    
    //timeVO 클래스를 생성하여 시간정보를 읽어오는 수행문
    TimeVO timeVO = new TimeVO();
    timeVO.setTimeField("timeField");
    timeVO.setMetaField("metaField");
    timeVO.setGranularity("granularity");
    
    //JSON 데이터 형식으로 VO객체를 변환
    String jsonData = objectMapper.writeValueAsString(timeVO);
    System.out.println(jsonData);   
    
    Properties properties = new Properties(); 
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("retries", 0);
    properties.put("batch.size", 16384);
    properties.put("linger.ms", 1);
    properties.put("buffer.memory", 33554432);
    properties.put("block.on.buffer.full", "true");
    // 문자열 타입 Json 데이터 저장예시 
    // String json = "{\"1\":\"2022-08-05 10:17:20\", \"2\":\"2022-08-05 10:17:25\", 
    // \"3\":\"2022-08-05 10:17:30\"}";
    properties.put("1", "2022-08-05 10:17:20");
    properties.put("2", "2022-08-05 10:17:25");
    properties.put("3", "2022-08-05 10:17:30");
    properties.put(jsonData, "timeVO.class");
    String key = "key";
    ProducerRecord<String, String> record1313 = new ProducerRecord<>(topic, key, jsonData);
    Producer<String, String> producer2 = new KafkaProducer<>(properties);

    producer2.send(record1313, new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error while producing message to topic:" + recordMetadata);
                e.printStackTrace();
            } else {
                String prdcMessage = String.format("Produced record to topic %s partition [%d] @ offset %d",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                System.out.printf("%s%n", prdcMessage);
            }
        }

      //클라이언트 스토리지 설정 
      AdminClient adminClient = AdminClient.create(properties);   

      try {
          //토픽 객체에 콜렉션 "logs"새로운 토픽  추가 
        while(true) {
          adminClient.(Collections.singleton(adminClient));
          Thread.sleep(1000);
        }
        
      
        //json 형식으로 CurrentTime필드로 저장된 메시지 정보를 생성한다
        //json 시간 정보 입력 예시  : {"timeField":"2020-01-01T00:00:00.000Z","metaField":"metaField","granularity":"granularity"}
         Map<String, String> objMap =  new HashMap<>();
         System.out.println(jsonData);

      //중간에 예외가 발생하면 익셉션을 처리하고 아니면 예외를 객체를 반환하는 익셉션을 수행한다
      } catch ( Exception e) {

          // 토픽 발생을 실패할 경우 런타임 예외를 발생시킨다
          if (!(e.getCause() instanceof TopicExistsException)) {
              throw new RuntimeException(e);
          }else{
              System.out.println("토픽이 이미 존재합니다.");
          }
      } finally {
          adminClient.close();
      }
  });

}