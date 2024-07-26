package com.myorg;

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sns.subscriptions.SqsSubscription;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;



public class SnsSqsProcessorStack extends Stack {
    public SnsSqsProcessorStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public SnsSqsProcessorStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        // The code that defines your stack goes here

        // Create SNS topic
        Topic topic = Topic.Builder.create(this, "MyTopic")
                .topicName("MyTopic")
                .build();

        // Create Dead letter queue
        Queue dlq = Queue.Builder.create(this, "DeadLetterQueue")
                .queueName("DeadLetterQueue")
                .build();

        // Create main SQS queue
        Queue queue = Queue.Builder.create(this, "MainQueue")
                .queueName("MainQueue")
                .deadLetterQueue(DeadLetterQueue.builder()
                        .queue(dlq)
                        .maxReceiveCount(3)
                        .build())
                .build();

        // Subscribe SQS queue to SNS topic
        topic.addSubscription(new SqsSubscription(queue));

        // Output ARN and URLs
        CfnOutput.Builder.create(this, "TopicArn")
                .description("The Arn of SNS topic")
                .value(topic.getTopicArn())
                .build();

        CfnOutput.Builder.create(this, "QueueUrl")
                .description("The main queue url")
                .value(queue.getQueueUrl())
                .build();

        CfnOutput.Builder.create(this, "DlqUrl")
                .description("The dlq url")
                .value(dlq.getQueueUrl())
                .build();
    }
}
