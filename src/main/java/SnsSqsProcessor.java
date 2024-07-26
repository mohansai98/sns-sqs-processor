import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SnsSqsProcessor {

    private final String topicArn;
    private final String queueUrl;
    private final String dlqUrl;
    private final SnsClient snsClient;
    private final SqsClient sqsClient;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public SnsSqsProcessor(String topicArn, String queueUrl, String dlqUrl) {
        this.topicArn = topicArn;
        this.queueUrl = queueUrl;
        this.dlqUrl = dlqUrl;
        this.snsClient = SnsClient.builder().region(Region.US_EAST_2).build();
        this.sqsClient = SqsClient.builder().region(Region.US_EAST_2).build();
    }

    public void publishMessage(String message) {
        PublishRequest request = PublishRequest.builder()
                .message(message)
                .topicArn(topicArn)
                .build();
        try {
            snsClient.publish(request);
            System.out.println("Message published: "+message);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }
    }

    public void processMessage() {
        while (running.get()) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20)
                    .build();
            List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
            for (Message message : messages) {
                if(!running.get()) {
                    break;
                }
                try {
                    System.out.println("Processing message: "+message.body());
                    processMessage(message);
                    DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .receiptHandle(message.receiptHandle())
                            .build();
                    sqsClient.deleteMessage(deleteRequest);
                } catch (Exception e) {
                    System.out.println("Error processing message: "+e.getLocalizedMessage());
                }
            }
        }
        System.out.println("Message processing stopped.");
    }

    private void processMessage(Message message) {

        if(message.body().contains("error")) {
            throw new RuntimeException("Error processing message");
        }
    }

    public void checkDeadLetterQueue() {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(dlqUrl)
                .maxNumberOfMessages(10)
                .build();
        List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
        System.out.println("Messages in Dead Letter Queue: ");
        for (Message message: messages) {
            System.out.println(message.body());
        }
    }

    public void shutdown() {
        running.set(false);
        System.out.println("Shutting down message processor...");
    }

    public static void main(String[] args) {
        String topicArn = System.getenv("TOPIC_ARN");
        String queueUrl = System.getenv("QUEUE_URL");
        String dlqUrl = System.getenv("DLQ_URL");

        SnsSqsProcessor processor = new SnsSqsProcessor(topicArn, queueUrl, dlqUrl);

        processor.publishMessage("Hello World");
        processor.publishMessage("This message has error");
        processor.publishMessage("Another message");

        Thread processingThread = new Thread(processor::processMessage);
        processingThread.start();

        try {
            Thread.sleep(30000);
        } catch (InterruptedException e){
            System.out.println(e.getLocalizedMessage());
        }

        processor.shutdown();

        try {
            processingThread.join();
        } catch (InterruptedException e){
            System.out.println(e.getLocalizedMessage());
        }

        processor.checkDeadLetterQueue();
        System.out.println("Application shutdown");

    }
}
