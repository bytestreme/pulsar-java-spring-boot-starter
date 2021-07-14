package io.github.majusko.pulsar;

import io.github.majusko.pulsar.properties.ConsumerProperties;
import io.github.majusko.pulsar.properties.PulsarProperties;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
@ComponentScan
@EnableConfigurationProperties({PulsarProperties.class, ConsumerProperties.class})
public class PulsarAutoConfiguration {

    private final PulsarProperties pulsarProperties;

    public PulsarAutoConfiguration(PulsarProperties pulsarProperties) {
        this.pulsarProperties = pulsarProperties;
    }

    @Bean
    @ConditionalOnMissingBean
    public PulsarClient pulsarClient() throws PulsarClientException {
        if (pulsarProperties.getAuthentication() == null
                || pulsarProperties.getAuthentication().isEmpty()
                || pulsarProperties.getServiceUrl() == null
                || pulsarProperties.getServiceUrl().isEmpty()) {
            throw new IllegalStateException("provide serviceUrl and authentication");
        }
        return PulsarClient.builder()
                .serviceUrl(pulsarProperties.getServiceUrl())
                .authentication(
                        AuthenticationFactory.token(pulsarProperties.getAuthentication())
                )
                .build();
    }
}
