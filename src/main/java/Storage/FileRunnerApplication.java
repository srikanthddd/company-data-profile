package Storage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@SpringBootApplication(scanBasePackages = {"com.kalki.company_data_project", "Config", "Storage"})
public class FileRunnerApplication {

	public static void main(String[] args) throws IOException {
		ApplicationContext context = SpringApplication.run(FileRunnerApplication.class, args);

		FileUploadService fileUploadService = context.getBean(FileUploadService.class);
		PubSubMessagePublisher messagePublisher = context.getBean(PubSubMessagePublisher.class);

		String filePath = "/Users/srikanthdumpeti/Downloads/company-data-project/src/main/resources/googleNew2.json";
		File file = new File(filePath);
		long fileSizeInMB = file.length() / (1024 * 1024);

		String contentType = Files.probeContentType(Paths.get(filePath));

		String todayDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

		String cutoff = "1";

		String destinationFileName = "companyrawdata/" + todayDate + "_" + cutoff + ".json";

		fileUploadService.uploadFile(filePath, destinationFileName);

		String gcsFileLocation = "gs://srik-company-data-bucket/" + destinationFileName;

		System.out.println("File location in gcp to: " + gcsFileLocation);

		String topicName = "company-file-upload-topic";
		String bucketName = "srik-company-data-bucket";
		messagePublisher.publishMessage(topicName, bucketName, destinationFileName, fileSizeInMB, contentType);

	}
}
