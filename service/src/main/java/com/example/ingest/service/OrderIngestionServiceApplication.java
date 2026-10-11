package com.example.ingest.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class OrderIngestionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderIngestionServiceApplication.class, args);
    }
}
