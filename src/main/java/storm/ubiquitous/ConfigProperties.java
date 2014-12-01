/**
 * Used to store the configuration information
 * for the kafka consumer and redis .
 */


package storm.ubiquitous;

public interface ConfigProperties{

    final static String zkConnect = "localhost:2181";
    final static  String groupId = "group1";
    final static String topic = "try1";
    final static String kafkaServerURL = "localhost";
    final static int kafkaServerPort = 9092;
    final static int kafkaProducerBufferSize = 64*1024;
    final static int connectionTimeOut = 100000;
    final static int reconnectInterval = 10000;
    final static String topic2 = "topic2";
    final static String topic3 = "topic3";
    final static String clientId = "ClientId";
    final static String redisHost = "localhost";

    /* For Metrics */
    final static String GRAPHITE_HOST = "127.0.0.1";
    final static int CARBON_AGGREGATOR_LINE_RECEIVER_PORT = 2023;
    /* The following value must match carbon-cache's storage-schemas.conf */
    final static int GRAPHITE_REPORT_INTERVAL_IN_SECONDS = 10;
    final static String GRAPHITE_METRICS_NAMESPACE_PREFIX = "storm.ubiquitous.bolts";

}
