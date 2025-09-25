package com.hdfcbank.neftil.camt5254.processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;

@SpringBootApplication
@EnableRetry
public class NeftIlCamt5254ProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(NeftIlCamt5254ProcessorApplication.class, args);
    }

}
