

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class LocalApp {

    public static int get_N(){
        SqsClient   sqs = SqsClient.builder().region(Region.US_EAST_1).build();

        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName("step1")
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        Message m;
        while ((true)){
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .maxNumberOfMessages(1)
                    .queueUrl(queueUrl)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();

            if(messages.size()!=0) {
                m = messages.get(0);
                break;
            }
        }

        int N = Integer.parseInt(m.body());
       // delete_message(m, "step1");

        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(m.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);

        return N;
    }



    public static void main(String[] args) throws Exception {

        File stop_file = new File("stop_words.txt");
        aws aws_ = new aws();
        aws_.create_s3();
        aws_.createBucket("stopwordsbgu");
        aws_.send_file("stopwordsbgu","stop",stop_file);


        Configuration conf = new Configuration();
        Job job = new Job(conf, "word count");
        job.setJarByClass(Step_one.class);
        job.setMapperClass(Step_one.MapperClass.class);
        job.setPartitionerClass(Step_one.PartitionerClass.class);
        job.setCombinerClass(Step_one.CombinerClass.class);
        job.setReducerClass(Step_one.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

      // job.setInputFormatClass(SequenceFileInputFormat.class);   *uncomment for a sequence input file*
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("step1_output"));
        int state = job.waitForCompletion(true) ? 0 : 1;
        if(state!=0){
            System.exit(1);
        }
        Long N = aws_.get_N("N");

        Configuration conf2 = new Configuration();
        conf2.set("N",String.valueOf(N));
        Job job2 = new Job(conf2, "word count");
        job2.setJarByClass(Step_two.class);
        job2.setMapperClass(Step_two.MapperClass.class);
        job2.setPartitionerClass(Step_two.PartitionerClass.class);
        job2.setReducerClass(Step_two.ReducerClass.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path("step1_output"));

        FileOutputFormat.setOutputPath(job2, new Path("step2_output"));

        int state2 = job2.waitForCompletion(true) ? 0 : 1;
        if(state2!=0){
            System.exit(1);
        }

        Configuration conf3 = new Configuration();
        Job job3 = new Job(conf3, "word count");
        job3.setJarByClass(Step_three.class);
        job3.setMapperClass(Step_three.MapperClass.class);
        job3.setPartitionerClass(Step_three.PartitionerClass.class);
        job3.setReducerClass(Step_three.ReducerClass.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path("step2_output"));

        FileOutputFormat.setOutputPath(job3, new Path("step3_output"));

        int state3 = job3.waitForCompletion(true) ? 0 : 1;
        if(state3!=0){
            System.exit(1);
        }

        Configuration conf4 = new Configuration();
        conf4.set("minPmi",args[0]);
        conf4.set("rel_minPmi",args[1]);

        Job job4 = new Job(conf4, "word count");

        job4.setJarByClass(Step_four.class);
        job4.setMapperClass(Step_four.MapperClass.class);
        job4.setPartitionerClass(Step_four.PartitionerClass.class);
        job4.setCombinerClass(Step_four.CombinerClass.class);
        job4.setReducerClass(Step_four.ReducerClass.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);gi
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job4, new Path("step3_output"));

        FileOutputFormat.setOutputPath(job4, new Path("output"));

        int state4 = job4.waitForCompletion(true) ? 0 : 1;
        if(state4!=0){
            System.exit(1);
        }



    }

}
