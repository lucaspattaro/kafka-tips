package testepato.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String  bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);

        //Runnable myConsumerThread = new ConsumerThread(bootstrapServer, groupId, topic, latch);

    }



    public class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());


        public ConsumerThread(String bootstrapServer,  String groupId, String topic, CountDownLatch latch){

            //create consumer configs

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            this.latch = latch;
            //create consumer
            KafkaConsumer<String , String > consumer = new KafkaConsumer<String, String>(properties);
            //subscribe consumer to our topics
            consumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run() {
            //pool for new data
            try {

            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records){
                    logger.info("Key: " + record.key() + "value: " + record.value());
                    logger.info("Partition: " + record.partition() + "Offset: " + record.offset());
                }
            }
            } catch (WakeupException e) {
                logger.info("Receive Shutdown signal");
            } finally {
                consumer.close();
                //tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown(){
            //method to interrupt consumer.pool()
            //
            consumer.wakeup();

        }
    }
}
