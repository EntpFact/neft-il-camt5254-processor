package com.hdfcbank.neftil.camt5254.processor.kafkaproducer;

import com.hdfcbank.messageconnect.config.PubSubOptions;
import com.hdfcbank.messageconnect.dapr.producer.DaprProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class KafkaUtilsTest {

    @InjectMocks
    private KafkaUtils kafkaUtils;

    @Mock
    private DaprProducer daprProducer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        // Inject test value for dispatcherTopic
        ReflectionTestUtils.setField(kafkaUtils, "dispatcherTopic", "test-topic");
    }

    @Test
    void testPublishToResponseTopic_Success() {
        // Mock daprProducer to return a successful Mono
        when(daprProducer.invokeDaprPublishEvent(any(PubSubOptions.class)))
                .thenReturn(Mono.just("SUCCESS"));

        kafkaUtils.publishToResponseTopic("test-message");

        // Verify daprProducer was called once
        verify(daprProducer, times(1)).invokeDaprPublishEvent(any(PubSubOptions.class));
    }

    @Test
    void testPublishToResponseTopic_Error() {
        // Mock daprProducer to return an error
        when(daprProducer.invokeDaprPublishEvent(any(PubSubOptions.class)))
                .thenReturn(Mono.error(new RuntimeException("Kafka error")));

        kafkaUtils.publishToResponseTopic("test-message");

        // Verify daprProducer was still called
        verify(daprProducer, times(1)).invokeDaprPublishEvent(any(PubSubOptions.class));
    }
}
