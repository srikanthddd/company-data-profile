package Storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class PubSubMessagePublisher {

    @Autowired
    private PubSubTemplate pubSubTemplate;

    @Autowired
    private ObjectMapper  objectMapper;

    public void publishMessage(String topicName, String bucketName, String filePath, long fileSize, String contentType) {

        try{
            Map<String, Object> messagePayload = new HashMap<>();
            messagePayload.put("bucketName", bucketName);
            messagePayload.put("filePath", filePath);
            messagePayload.put("fileSize", fileSize + " MB");
            messagePayload.put("contentType", contentType);
            messagePayload.put("project_id","publishingengine");

            String messageJson = objectMapper.writeValueAsString(messagePayload);

            pubSubTemplate.publish(topicName, messageJson);

            System.out.println("Published Json message : "+messageJson);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
