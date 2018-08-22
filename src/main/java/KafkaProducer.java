/**
 * @author SampathD
 * @created 09/08/2018
 */

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class KafkaProducer
{
  private final static String TOPIC = "test2";

  private final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

  private static Producer<Long, String> createProducer()
  {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new org.apache.kafka.clients.producer.KafkaProducer<Long, String>(props);
  }

  public static void runProducerSync(final int sendMessageCount)
  { 
    final Producer<Long, String> producer = createProducer();
    long time = System.currentTimeMillis();

    try
    {
      for (long index = time; index < time + sendMessageCount; index++)
      {
        final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC, index,
            "Hello Mom " + index);

        RecordMetadata metadata = producer.send(record).get();

        long elapsedTime = System.currentTimeMillis() - time;
        System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record.key(),
            record.value(), metadata.partition(), metadata.offset(), elapsedTime);

      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    finally
    {
      producer.flush();
      producer.close();
    }
  }

  public static void runProducer()
  {
    final Producer<Long, String> producer = createProducer();
    long time = System.currentTimeMillis();

    long counter = 1;
    try
    {
      while (true)
      {
        final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, counter, "Message-" + counter);
        RecordMetadata metadata = producer.send(record).get();
        long elapsedTime = System.currentTimeMillis() - time;
        System.out.println(
            "Sent to topic - key: " + record.key() + " value: " + record.value() + " partition: " + metadata.partition()
                + " offset: " + metadata.offset() + " time: " + elapsedTime);
        if (counter == 500)
        {
          break;
        }
        counter++;
      }
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
    catch (ExecutionException e)
    {
      e.printStackTrace();
    }
    finally
    {
      producer.flush();
      producer.close();
    }
  }

  public static void main(String[] args)
  {
    KafkaProducer.runProducer();
  }

}
