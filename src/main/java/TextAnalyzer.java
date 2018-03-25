/* EE 360P HW4
 * Name: Xiangxing Liu
 * EID: xl5587
 * Name: Kravis Cho
 * EID: kyc375
 */
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.*;

// Do not change the signature of this class
public class TextAnalyzer extends Configured implements Tool {

    // Replace "?" with your own output key / value types
    // The four template data types are:
    //     <Input Key Type, Input Value Type, Output Key Type, Output Value Type>
    public static class TextMapper extends Mapper<LongWritable, Text, Text, Tuple> {

        //private final IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            line = line.toLowerCase();
            line = line.replaceAll("[^A-Za-z0-9]", " ");
            StringTokenizer token = new StringTokenizer(line);
            ArrayList<String> wordList = new ArrayList<String>();

            while(token.hasMoreTokens()){
                String temp = token.nextToken();
                wordList.add(temp);
            }

            Set<String> wordSet = new HashSet<String>();
            for(int i = 0; i < wordList.size(); i++){
                String tmp = wordList.get(i);
                if(!wordSet.contains(tmp)) {
                    wordSet.add(tmp);
                    Tuple t;
                    for (int j = 0; j < wordList.size(); j++) {
                        if (i != j) {
                            String count = wordList.get(j);
                            t = new Tuple(count, 1);
                            context.write(new Text(tmp), t);
                        }
                    }
                }
            }
        }
    }

    // Replace "?" with your own key / value types
    // NOTE: combiner's output key / value types have to be the same as those of mapper
    public static class TextCombiner extends Reducer<Text, Tuple, Text, Tuple> {
        public void reduce(Text key, Iterable<Tuple> tuples, Context context)
                throws IOException, InterruptedException
        {
            Iterator<Tuple> tup = tuples.iterator();
            Map<String, Integer> map = new HashMap<String, Integer>();
            while(tup.hasNext()){
                Tuple test = tup.next();
                String tmp = test.getWord();
                if (map.containsKey(tmp)) {
                    map.put(tmp, test.getCount() + map.get(tmp));
                }else{
                    map.put(tmp, test.getCount());
                }
            }
            Set<String> i = map.keySet();
            for(String k : i){
                context.write(key, new Tuple(k, map.get(k)));
            }
        }
    }

    // Replace "?" with your own input key / value types, i.e., the output
    // key / value types of your mapper function
    public static class TextReducer extends Reducer<Text, Tuple, Text, Text> {
        private final static Text emptyText = new Text("");
        private Text queryWordText = new Text();

        public void reduce(Text key, Iterable<Tuple> queryTuples, Context context)
                throws IOException, InterruptedException
        {
            // Implementation of you reducer function
            Map<String, Integer> map = new TreeMap<String, Integer>();
            Iterator<Tuple> tup = queryTuples.iterator();
            while(tup.hasNext()){
                Tuple temp = tup.next();
                String tmp = temp.getWord();
                if(map.containsKey(tmp)){
                    map.put(tmp, temp.getCount() + map.get(tmp));
                } else {
                    map.put(tmp, temp.getCount());
                }
            }
            // Write out the results; you may change the following example
            // code to fit with your reducer function.
            //   Write out the current context key
            context.write(key, emptyText);
            //   Write out query words and their count
            for(String queryWord: map.keySet()){
                String count = map.get(queryWord).toString() + ">";
                queryWordText.set("<" + queryWord + ",");
                context.write(queryWordText, new Text(count));
            }
            //   Empty line for ending the current context key
            context.write(emptyText, emptyText);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "xl5587_kyc375"); // Replace with your EIDs
        job.setJarByClass(TextAnalyzer.class);

        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);
        // Uncomment the following line if you want to use Combiner class
        job.setCombinerClass(TextCombiner.class);
        job.setReducerClass(TextReducer.class);

        // Specify key / value types (Don't change them for the purpose of this assignment)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // If your mapper and combiner's  output types are different from Text.class,
        // then uncomment the following lines to specify the data types.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Tuple.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // Do not modify the main method
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(res);
    }

    /* Subclass Tuple */
    public static class Tuple implements WritableComparable<Tuple>{
        private Text word;
        private IntWritable count;

        public Tuple(){
            this.word = new Text();
            this.count = new IntWritable();
        }

        public Tuple(Text word, IntWritable count){
            this.word = word;
            this.count = count;
        }

        public Tuple(String str, int i){
            this.word = new Text(str);
            this.count = new IntWritable(i);
        }

        public String getWord(){
            return this.word.toString();
        }

        public int getCount(){
            return this.count.get();
        }

        public void write(DataOutput dataOutput) throws IOException {
            word.write(dataOutput);
            count.write(dataOutput);
        }

        public void readFields(DataInput dataInput) throws IOException {
            word.readFields(dataInput);
            count.readFields(dataInput);
        }

        public int compareTo(Tuple o) {
            int ret = word.compareTo(o.word);

            if (ret != 0) {
                return ret;
            }

            return count.compareTo(o.count);
        }
    }
}

