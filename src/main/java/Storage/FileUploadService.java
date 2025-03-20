package Storage;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Service
public class FileUploadService {

    private final Storage storage;

    @Value("${spring.cloud.gcp.storage.bucket}")
    private String bucketName;

    public FileUploadService(Storage storage) {
        this.storage = storage;
    }

    public void uploadFile(String filePath, String destinationFileName) throws IOException {
        Bucket bucket = storage.get(bucketName);

        Path path = Paths.get(filePath);
        byte[] bytes = Files.readAllBytes(path);

        Blob blob = bucket.create(destinationFileName, bytes);
        System.out.println("File uploaded to " + destinationFileName);
    }

}
