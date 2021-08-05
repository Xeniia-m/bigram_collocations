

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class aws {

    public static S3Client s3;
    public static SqsClient sqs;

    public void createBucket(String bucket) {
        Region region = Region.US_EAST_1;

        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .build());



    }

    public void create_s3() {
        Region region = Region.US_EAST_1;
        s3 = S3Client.builder().region(region).build();


    }

    public void create_sqs(){
        sqs = SqsClient.builder().region(Region.US_EAST_1).build();

    }

    public void send_file(String bucket, String key, File input_file) throws IOException {

        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key)
                        .build(),
                RequestBody.fromBytes((Files.readAllBytes(input_file.toPath()))));

    }

    public void get_object(String bucket, String key, String file_name){
        s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(),
                ResponseTransformer.toFile(Paths.get(file_name)));

    }

    public void create_queue(String queue_name) {

        sqs = SqsClient.builder().region(Region.US_EAST_1).build();

        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queue_name)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            //throw e;

        }

    }

    public void send_message(String message, String queue_name) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue_name)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);

    }

    public  Long get_N(String queue_name){
        sqs = SqsClient.builder().region(Region.US_EAST_1).build();

        // receive messages from the queue
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue_name)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        for (Message m : messages) {
                Long N = Long.parseLong(m.body());
                delete_message(m,queue_name);
               return N;
        }
        return Long.parseLong("-1");
    }

    public void delete_folder(String bucket, String folder) {
        ListObjectsV2Request listObjectsReqManual = ListObjectsV2Request.builder()
                .bucket(bucket)
                .maxKeys(1)
                .prefix(folder)
                .build();
        ListObjectsV2Response listObjResponse = s3.listObjectsV2(listObjectsReqManual);
        List<String> keysList = new LinkedList<String>();//[ listObjResponse.contents().size() ];
        listObjResponse.contents().stream()
                .forEach(content -> keysList.add(content.key()));
        for (String key : keysList) {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(key).build();
            s3.deleteObject(deleteObjectRequest);
        }

    }

    public static String get_url(String queue_name) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue_name)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        return queueUrl;
    }

    private static void delete_message(Message message, String queue_name) {
        String queueUrl = get_url(queue_name);

        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
    }

    public void print_messages(String queue){
         String queueUrl = get_url(queue);

        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        for (Message m : messages) {
            System.out.println(m.body());
            delete_message(m, queue);

        }
    }

}
