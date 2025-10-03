package com.hdfcbank.neftil.camt5254.processor.config;

import com.hdfcbank.messageconnect.config.DaprMessageConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;
import org.springframework.retry.annotation.EnableRetry;

@Slf4j
@EnableConfigurationProperties(RetryProperties.class)
@EnableRetry
@Import({DaprMessageConfig.class})
public class NeftIlCamt5254ProcessorConfig {


}
