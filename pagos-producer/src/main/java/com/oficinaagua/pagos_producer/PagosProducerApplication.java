package com.oficinaagua.pagos_producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableFeignClients
public class PagosProducerApplication {

	public static void main(String[] args) {

		SpringApplication.run(PagosProducerApplication.class, args);
	}

}
