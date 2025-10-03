package com.hdfcbank.neftil.camt5254.processor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "retry.transaction-audit")
public class RetryProperties {
    private Integer maxAttempts;   // wrapper, allows null
    private Long delay;
    private Double multiplier;
}

