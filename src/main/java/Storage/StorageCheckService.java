package Storage;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;

@Service
public class StorageCheckService {

    private final Storage storage;

    @Value("${spring.cloud.gcp.storage.bucket}")
    private String bucketName;

    public StorageCheckService(Storage storage) {
        this.storage = storage;
    }

    public void listFiles() {
        Bucket bucket = storage.get(bucketName);
        System.out.println("Files in bucket:");
        bucket.list().iterateAll().forEach(blob -> System.out.println(blob.getName()));
    }


}
