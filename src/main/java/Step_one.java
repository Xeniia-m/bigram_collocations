

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

import java.io.*;
import java.util.HashMap;

public class Step_one {
    public static boolean illegal_word(String word){
        for(int i=0;i<word.length();i++){
            if(((int)(word.charAt(i))>32) && ((int)(word.charAt(i))<65)){
                return true;
            }
        }
        return false;
    }
    public static class MapperClass extends Mapper<LongWritable, Text,  Text, Text> {
        private HashMap<String, Integer> stop_words = new HashMap<String, Integer>();

        public int get_decade (int year){
            year = year  /10;
            return year;
        }

        @Override
        public void setup(Context context) throws IOException {
            aws aws_ = new aws();
            aws_.create_s3();
            aws_.get_object("stopwordsbgu","stop","stop_words_");

            File file = new File("stop_words_");
            FileInputStream fstream = new FileInputStream(file);
         //   FileReader fr = new FileReader(file);  //Creation of File Reader object
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream,"UTF-8")); //Creation of BufferedReader object
            String s;
            while((s=br.readLine())!=null)  {
                String[] words = s.split("\\s+");
                for(String word: words){
                    stop_words.put(word,1);
                }
            }
            fstream.close();

        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           /*word1 word2  num -->
                word1 word2 num
                word1 *     num
                * word2     num
            */
            String[] bigram = value.toString().split("\t");
         //   System.out.println(bigram[0]);
            if (bigram.length >= 5) {
                IntWritable decade = new IntWritable(get_decade(Integer.parseInt(bigram[1])));
                String[] big_words = bigram[0].split("\\s+");
                if (big_words.length == 2) {
                    if (stop_words.containsKey(big_words[0]) || stop_words.containsKey(big_words[1])) {
                        return;
                    }

                    if(illegal_word(big_words[0]) || illegal_word(big_words[1])){
                        return;
                    }
                    context.write(new Text(String.valueOf(decade.get() + "\t" + big_words[0] + "\t" + big_words[1])), new Text(bigram[2]));
                    context.write(new Text(String.valueOf(big_words[0] + "\t*")), new Text(bigram[2]));
                    context.write(new Text(String.valueOf("*\t" + big_words[1])), new Text(bigram[2]));
                    context.write(new Text(String.valueOf("N")), new Text(bigram[2]));

                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }
    }

    public static class ReducerClass extends Reducer<Text,Text, Text, Text> {
        @Override
        /*- for each decade for each bigram gets the number of occurrences
        - for each word w that there is a bigram that starts with the word w, gets the number of bigrams
        that starts with the word
         - for each word w that there is a bigram that ends with the word w, gets the number of bigrams
        that ends with the word
        - gets the number N of all bigrams and sends it to sqs

         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long count=0;
            for(Text value: values) {
                count+=Long.parseLong(value.toString());
            }
            if(key.toString().equals("N")){
                aws aws_ = new aws();
                aws_.create_queue("N");
                aws_.send_message(Long.toString(count),"N");
            }
            else
                context.write(key,new Text(String.valueOf(count)));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }
    }
    public static class CombinerClass extends Reducer<Text,Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long count=0;
            for(Text value: values) {
                count+=Long.parseLong(value.toString());
            }

                context.write(key,new Text(String.valueOf(count)));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }
    }

    public static class PartitionerClass extends Partitioner<Text,Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
               return Math.abs(key.toString().split("\t")[0].hashCode() % numPartitions);
            }
        }

    public static void main(String[] args) throws Exception {

        String input_path = args[0];
        String output_path = args[1];
        System.out.println("Step One!");
        Configuration conf = new Configuration();
        Job job = new Job(conf, "step1");
        job.setJarByClass(Step_one.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(Step_one.MapperClass.class);
        job.setPartitionerClass(Step_one.PartitionerClass.class);
        job.setCombinerClass(Step_one.CombinerClass.class);
        job.setReducerClass(Step_one.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //

        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));
        System.out.println("step one starting..");
        job.waitForCompletion(true) ;
        System.out.println("step one end");


    }

    }
