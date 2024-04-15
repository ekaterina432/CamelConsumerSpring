package camel.example.config;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.component.ComponentsBuilderFactory;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.spring.boot.CamelContextConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:application.properties")
public class ConsumerConfiguration {

    @Value("${kafka.brokers}")
    private String kafkaBrokers;

    @Value("${consumer.topic}")
    private String consumerTopic;

    @Value("${consumer.maxPollRecords}")
    private Integer maxPollRecords;

    @Value("${consumer.consumersCount}")
    private Integer consumersCount;

    @Value("${consumer.seekTo}")
    private String seekTo;

    @Value("${consumer.group}")
    private String consumerGroup;

    @Bean
    public RouteBuilder kafkaRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("kafka:" + consumerTopic
                        + "?maxPollRecords=" + maxPollRecords
                        + "&consumersCount=" + consumersCount
                        + "&seekTo="+ seekTo
                        + "&groupId=" + consumerGroup)
                        // логирует содержимое тела каждого сообщения
                        .routeId("FromKafka")
                        .log("${body}");
            }
        };
    }

    @Bean
    CamelContextConfiguration contextConfiguration(RouteBuilder kafkaRouteBuilder) {
        return new CamelContextConfiguration() {
            @Override
            public void beforeApplicationStart(CamelContext camelContext) {
                // Настройка компонента Kafka
                setUpKafkaComponent(camelContext);
            }

            @Override
            public void afterApplicationStart(CamelContext camelContext) {
                try {
                    Thread.sleep(50_000);
                    camelContext.addStartupListener((camelContext1, alreadyStarted) -> {
                        if (!alreadyStarted) {
                            System.out.println("Camel application started, reading messages from Kafka...");
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            private void setUpKafkaComponent(CamelContext camelContext) {
                ComponentsBuilderFactory.kafka()
                        .brokers(kafkaBrokers)
                        .register(camelContext, "kafka");
            }
        };
    }
}
