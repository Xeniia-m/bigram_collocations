

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Step_two {
    public static class MapperClass extends Mapper<LongWritable, Text,  Text, Text> {

        @Override
        public void setup(Context context) throws IOException {

        }
        /*
        for each bigram: word1 word2 num
        write to the contex:
        key: decade word1 *     value: word1 word2 num

        for each word1 * num
        write to the contex:
        key: decade word1 *     value: num


         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split("\t");

            if ( vals.length > 3){//(!vals[1].equals("*") && !vals[0].equals("*")) {
                context.write(new Text(vals[1] + " * 1"), new Text(vals[0] + " " + vals[1] + " " + vals[2] + " " + vals[3]));
            } else
                context.write(new Text(vals[0] + " " + vals[1] + " 0"),
                        new Text(vals[2]));

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }
    }

    public static class ReducerClass extends Reducer<Text,Text, Text, Text> {
        private  long count=-1;
        private  long N = -1;
        @Override
        /*
        first gets from map the number of bigrams that starts with the word (word * num)
        and N
        (because of 0 in the key the reducer gets it first)
        and then gets from map all the bigrams that starts with the word
        then write to the contex for all this bigrams:
        decade word1 word2 N num_of_bigrams num_of_word1
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //long count = -1;
            N = Long.parseLong(context.getConfiguration().get("N"));
            String[] keys = key.toString().split(" ");

            if(keys[0].equals("*")){
                for(Text value: values) {
                    String[] vals = value.toString().split(" ");
                    context.write(new Text(keys[0] + "\t" + keys[1] ),new Text(vals[0]));
                }
            }
            else {
                if (keys[2].equals("0")) {
                    for (Text value : values) {
                        String[] vals = value.toString().split(" ");
                        System.out.println("key: " + keys[0] + " " + keys[1] + " " + vals[0]);
                        count = Long.parseLong(vals[0]);

                    }
                } else {
                    for (Text value : values) {
                        String[] vals = value.toString().split(" ");
                        context.write(new Text(vals[0]), new Text(vals[1] + "\t" + vals[2] + "\t" +N+"\t"+ vals[3] + "\t" + String.valueOf(count)));

                    }
                }
            }
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }
    }

    public static class PartitionerClass extends Partitioner<Text,Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String keys[] = key.toString().split(" ");
              return Math.abs(keys[0].hashCode() % numPartitions);

        }
    }

    public static void main(String[] args) throws Exception {
        String input_path = args[0];
        String output_path = args[1];
        Configuration conf2 = new Configuration();
        aws aws_ = new aws();
        Long N = aws_.get_N("N");

        conf2.set("N",String.valueOf(N));

        Job job2 = new Job(conf2, "step2");
        job2.setJarByClass(Step_two.class);
        job2.setMapperClass(Step_two.MapperClass.class);
        job2.setPartitionerClass(Step_two.PartitionerClass.class);
        job2.setReducerClass(Step_two.ReducerClass.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(input_path));

        FileOutputFormat.setOutputPath(job2, new Path(output_path));

        int state2 = job2.waitForCompletion(true) ? 0 : 1;
        if(state2!=0){
            System.exit(1);
        }


    }

    }
