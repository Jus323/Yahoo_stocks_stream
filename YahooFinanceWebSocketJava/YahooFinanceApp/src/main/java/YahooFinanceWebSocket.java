import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.Properties;

public class YahooFinanceWebSocket {
    private static final String TOPIC = "stocks";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private KafkaProducer<String, String> producer;

    public YahooFinanceWebSocket() {
        this.producer = createProducer();
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "YahooFinanceProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

// Add debugging configs
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, "5000"); // Check metadata more frequently
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000");    // Fail faster for debugging
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000"); // Shorter timeout for debugging

// Enable debug logging
        props.put("logger.level", "DEBUG");

        return new KafkaProducer<>(props);
    }

    public void start() {
        try {
            URI uri = new URI("wss://streamer.finance.yahoo.com/");
            WebSocketClient client = new WebSocketClient(uri) {
                @Override
                public void onOpen(ServerHandshake handshake) {
                    System.out.println("Connected to Yahoo Finance WebSocket");
                    String subscriptionMessage = "{\"subscribe\": [\"D05.SI\", \"Z74.SI\"]}";
                    send(subscriptionMessage);
                    System.out.println("Subscribed to symbols: D05.SI, Z74.SI");
                }

                @Override
                public void onMessage(String message) {
                    try {
                        byte[] decodedBytes = Base64.getDecoder().decode(message);
                        TickerOuterClass.Ticker ticker = TickerOuterClass.Ticker.parseFrom(decodedBytes);

                        String key = ticker.getId();
                        String value = String.format("{\"time\":%d,\"price\":%.2f}",
                                ticker.getTime(), ticker.getPrice());

                        System.out.println(ticker.getPrice());

                        ProducerRecord<String, String> record =
                                new ProducerRecord<>(TOPIC, key, value);

                        producer.send(record, (metadata, exception) -> {
                            if (exception == null) {
                                System.out.printf("Produced record to topic %s partition %d offset %d%n",
                                        metadata.topic(), metadata.partition(), metadata.offset());
                            } else {
                                System.err.println("Error producing to Kafka: " + exception);
                            }
                        });
                    } catch (Exception e) {
                        System.err.println("Error processing message: " + e.getMessage());
                        e.printStackTrace();
                    }
                }

                @Override
                public void onClose(int code, String reason, boolean remote) {
                    System.out.println("Connection closed: " + reason);
                    producer.close();
                }

                @Override
                public void onError(Exception ex) {
                    System.err.println("Error: " + ex.getMessage());
                }
            };

            client.connect();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new YahooFinanceWebSocket().start();
    }
}