package com.hdfcbank.neftil.camt5254.processor.config;

import com.hdfcbank.messageconnect.config.DaprMessageConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({DaprMessageConfig.class})
public class NeftIlCamt5254ProcessorConfig {
}
