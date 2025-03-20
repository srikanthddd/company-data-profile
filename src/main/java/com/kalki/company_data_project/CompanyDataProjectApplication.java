package com.kalki.company_data_project;

import Storage.StorageCheckService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication(scanBasePackages = {"com.kalki.company_data_project","Config","Storage"})
public class CompanyDataProjectApplication {

	public static void main(String[] args) {
		ApplicationContext context = SpringApplication.run(CompanyDataProjectApplication.class, args);

		StorageCheckService storageService = context.getBean(StorageCheckService.class);

		storageService.listFiles();
	}

}
