/**
 * Used to store the configuration information
 * for the kafka consumer.
 */

package storm.ubiquitous.spouts;

public interface KafkaProperties
{
  final static String zkConnect = "192.168.43.80:2181";
  final static  String groupId = "group1";
  final static String topic = "topic1";
  final static String kafkaServerURL = "192.168.43.80";
  final static int kafkaServerPort = 9092;
  final static int kafkaProducerBufferSize = 64*1024;
  final static int connectionTimeOut = 100000;
  final static int reconnectInterval = 10000;
  final static String topic2 = "topic2";
  final static String topic3 = "topic3";
  final static String clientId = "ClientId";
}
