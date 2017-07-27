package ngram;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.PriorityQueue;


import java.io.IOException;
import java.util.*;

/**
 * Created by yizhu on 7/26/17.
 */
public class LanguageModel {

    public static class LanMap extends Mapper<LongWritable, Text, Text, Text> {
        int threshold;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            threshold = configuration.getInt("threshold", 20);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // This is\t20
            String line = value.toString().trim();
            // eliminate bad words
            String[] word_count = line.split("\t");
            if (word_count.length < 2) {
                return;
            }

            // eliminate count less than threshold
            int count = Integer.parseInt(word_count[1]);
            if (count < threshold) {
                return;
            }

            // This is cool \t 300
            // This is  cool=300
            String[] words = word_count[0].split("\\s+");
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                stringBuilder.append(words[i]);
                stringBuilder.append(" ");
            }
            String outputKey = stringBuilder.toString().trim();
            String outputValue = words[words.length - 1] + "=" + count;
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class LanReducer extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

        int topN;

        class WordCount {
            String word;
            int count;

            public WordCount(String word, int count) {
                this.word = word;
                this.count = count;
            }
        }

        class WordQueue extends PriorityQueue<WordCount> {
            @Override
            protected boolean lessThan(Object o, Object o1) {
                return ((WordCount) o).count < ((WordCount) o1).count;
            }

            public WordQueue(int size) {
                super();
                initialize(size);
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            topN = configuration.getInt("topN", 5);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // input: This is  cool=200, big=100, data=50
            // output into DB   This is | cool | 200
            //......
            TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
            for(Text val: values) {
                String curValue = val.toString().trim();
                String word = curValue.split("=")[0].trim();
                int count = Integer.parseInt(curValue.split("=")[1].trim());
                if(tm.containsKey(count)) {
                    tm.get(count).add(word);
                }
                else {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    tm.put(count, list);
                }
            }
            //<50, <girl, bird>> <60, <boy...>>
            Iterator<Integer> iter = tm.keySet().iterator();
            for(int j=0; iter.hasNext() && j < topN; j++) {
                int keyCount = iter.next();
                List<String> words = tm.get(keyCount);
                for(String curWord: words) {
                    context.write(new DBOutputWritable(key.toString(), curWord, keyCount),NullWritable.get());
                    j++;
                }
            }

        }
    }
}
