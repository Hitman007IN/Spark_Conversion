package com.myApplication;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkConverter {

    public static void main(String[] args) {
        System.out.println("inside SparkConverter...");
        SpringApplication.run(SparkConverter.class, args);
    }
}
