package com.dachen.eventanalysis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.dachen")
public class EventAnalysisApplication {

	public static void main(String[] args) {
		SpringApplication.run(EventAnalysisApplication.class, args);
	}
}
