package ngram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramLibraryBuilder {

    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        int gram;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            gram = configuration.getInt("gram", 5);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String sentence = value.toString().trim();
            sentence = sentence.replaceAll("[^a-z]"," ");
            String[] words = sentence.split("\\s+");

            if (words.length < 2) { // should throw an exception in real application
                return;
            }
            //This, is, cool, big, data
            // This is, is cool, cool big, big data
            // This is cool, is cool big, cool big data
            //......
            StringBuilder stringBuilder;
            for (int i = 0; i < words.length; i++) {
                stringBuilder = new StringBuilder();
                stringBuilder.append(words[i]);
                for (int n = 1; i + n < words.length && n < gram; n++) {
                    stringBuilder.append(" ");
                    stringBuilder.append(words[i + n]);
                    context.write(new Text(stringBuilder.toString().trim()), new IntWritable(1));
                }
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }
}
