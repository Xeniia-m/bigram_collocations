

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Step_three {
    public static class MapperClass extends Mapper<LongWritable, Text,  Text, Text> {

        @Override
        public void setup(Context context) throws IOException {

        }
        /*
        for all input: decade word1 word2 N num_bigrams num_word1
        write to the contex:
        key: decade * word2 1    value: word1 word2 N num_bigrams num_word1

        also  if we get   [decade * word2 num ] writes
        key: decade * word2 0    value: num
         */

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] vals = value.toString().split("\t");

            if(vals.length>3){
                context.write(new Text("* "+vals[2]+" 1"),
                        new Text(vals[0]+ " " +vals[1]+ " " + vals[2]+ " " + vals[3] + " "+ vals[4]+" "+vals[5]));
            }
            else
                context.write(new Text(vals[0]+" "+vals[1]+" 0"),
                        new Text(vals[2]));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }
    }
// decade bigram1 bigram2  N  num_of_bigram numfirst numsecond
    public static class ReducerClass extends Reducer<Text,Text, Text, Text> {
        private long count = -1;
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keys = key.toString().split(" ");
            System.out.println("new key:");
            if(keys[2].equals("0")) {
                for (Text value : values) {
                    String[] vals = value.toString().split(" ");
                    System.out.println("key: " + keys[0] + " " + keys[1] + " " + vals[0] );
                    count = Long.parseLong(vals[0]);

                }
            }
            else {
                for (Text value : values) {
                    String[] vals = value.toString().split(" ");
                    System.out.println("key: " + keys[0] + " " + vals[0] + " " + vals[1] + " " + vals[2] + " " + vals[3]);

                    context.write(new Text(vals[0]), new Text(vals[1] + "\t" + vals[2] + "\t" +
                            vals[3]+"\t"+vals[4] + "\t" + vals[5] + "\t" + count));
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
            return Math.abs(keys[1].hashCode() % numPartitions);

        }

    }

    public static void main(String[] args) throws Exception {
        String input_path = args[0];
        String output_path = args[1];
        Configuration conf3 = new Configuration();
        Job job3 = new Job(conf3, "step3");
        job3.setJarByClass(Step_three.class);
        job3.setMapperClass(Step_three.MapperClass.class);
        job3.setPartitionerClass(Step_three.PartitionerClass.class);
        job3.setReducerClass(Step_three.ReducerClass.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(input_path));

        FileOutputFormat.setOutputPath(job3, new Path(output_path));

        int state3 = job3.waitForCompletion(true) ? 0 : 1;
        if(state3!=0){
            System.exit(1);
        }

    }

    }
