package com.hdfcbank.neftil.camt5254.processor.kafkaproducer;

import com.hdfcbank.messageconnect.config.PubSubOptions;
import com.hdfcbank.messageconnect.dapr.producer.DaprProducer;
import com.hdfcbank.neftil.camt5254.processor.utils.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Slf4j
@Service
public class KafkaUtils {

    @Autowired
    DaprProducer daprProducer;

    @Value("${topic.dispatchertopic}")
    private String dispatcherTopic;


    public void publishToResponseTopic(String message) {

        var kafkaBinding = PubSubOptions.builder().requestData(message).topic(dispatcherTopic)
                .pubsubName(Constants.KAFKA_RESPONSE_TOPIC_DAPR_BINDING).build();
        var resp = daprProducer.invokeDaprPublishEvent(kafkaBinding);
        resp.doOnSuccess(res -> {
            log.info("Response published to response topic successfully");
        }).onErrorResume(res -> {
            log.info("Error on publishing the response to response topic");
            return Mono.empty();
        }).share().block();

    }
}
