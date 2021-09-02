

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Step_four {

    public static class MapperClass extends Mapper<LongWritable, Text,  Text, Text> {

        private double get_pmi(double N, double bigram_num, double first_word_num, double second_word_num) {
            return (Math.log10(bigram_num) + Math.log10(N) - Math.log10(first_word_num) - Math.log10(second_word_num));
        }

        private double get_npmi(double pmi, double N, double bigram_num) {
            double p = (double)bigram_num / (double)N;
            System.out.println("p = "+p);
            if (Double.compare(p,1.0)==0)
                return 1.0;
            double npmi = (pmi / (-Math.log10(p)));
            return npmi;
        }
        @Override
        public void setup(Context context) throws IOException {

        }
/*
    map gets: 
    decade  word1 word2     N   c(word1,word2)     c(word1 *)   c(* word2)
 */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] vals = value.toString().split("\t");
            String decade = vals[0];
            double bigram_num = Double.parseDouble(vals[4]);
            double first_word_num = Double.parseDouble(vals[5]);
            double second_word_num = Double.parseDouble(vals[6]);
            double N = Double.parseDouble(vals[3]);
            double pmi = get_pmi(N, bigram_num, first_word_num, second_word_num);
            double npmi = get_npmi(pmi, N, bigram_num);
           
            context.write(new Text(0+" "+decade),new Text(Double.toString(npmi))); //for second condition check
            double minPmi = Double.parseDouble(context.getConfiguration().get("minPmi", "1"));
            
            //first condition:
            if (npmi >= minPmi) {
                context.write(new Text(1+" "+decade+" "+(1-npmi)+" "+npmi),new Text(vals[1]+" "+vals[2]+" 1"));
            }
            else {
                context.write(new Text(1+" "+decade+" "+(1-npmi)+" "+npmi),new Text(vals[1]+" "+vals[2]+" 0"));
            }



            }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }
    }
    public static class ReducerClass extends Reducer<Text,Text, Text, Text> {
        private double count = 0;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           String[] key_ = key.toString().split(" ");
            if(key_[0].equals("0")){
                for(Text value: values){
                    double num = Double.parseDouble(value.toString());
                    count+=num;
                  
                }
               
            }
            else {
                for (Text value : values) {
                    String[] vals = value.toString().split(" ");
                    
                    //check if the first condition is passed
                    if (vals[2].equals("1")) {
                     context.write(new Text("decade:" + key_[1] + " " + vals[0]), new Text(vals[1] + "\t" + key_[3]));
                    }
                    //if not we need to check the second condition
                    else {
                        double npmi = Double.parseDouble(key_[3]);
                        double rel_minPmi = Double.parseDouble(context.getConfiguration().get("rel_minPmi", "1"));

                        if ((double)( npmi / count )>= rel_minPmi) {

                            context.write(new Text("decade:"+key_[1]+" "+vals[0]), new Text(vals[1]+"\t"+key_[3]));
                        }


                    }


                }

            }

        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }
    }
    //evaluates the npmi value in the same decade
    public static class CombinerClass extends Reducer<Text,Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double count = 0;
            String[] key_ = key.toString().split(" ");
            if (key_[0].equals("0")) {
                for (Text value : values) {
                    
                    double num = Double.parseDouble(value.toString());

                    count += num;
                   

                }
                context.write(key, new Text(Double.toString(count)));
            } else {
                for (Text value : values) {
                    context.write(key, value);
                }

            }
        }
    }
            public static class PartitionerClass extends Partitioner<Text,Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Integer.parseInt(key.toString().split(" ")[1]) % numPartitions;
        }
    }
    public static void main(String[] args) throws Exception {
        String input_path = args[0];
        String output_path = args[1];
        Configuration conf4 = new Configuration();
        conf4.set("minPmi", args[2]);
        conf4.set("rel_minPmi", args[3]);

        Job job4 = new Job(conf4, "step4");
        job4.setJarByClass(Step_four.class);
        job4.setMapperClass(Step_four.MapperClass.class);
        job4.setPartitionerClass(Step_four.PartitionerClass.class);
        job4.setCombinerClass(Step_four.CombinerClass.class);
        job4.setReducerClass(Step_four.ReducerClass.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job4, new Path(input_path));

        FileOutputFormat.setOutputPath(job4, new Path(output_path));

        int state4 = job4.waitForCompletion(true) ? 0 : 1;
        if (state4 != 0) {
            System.exit(1);
        }
     
    }

}
