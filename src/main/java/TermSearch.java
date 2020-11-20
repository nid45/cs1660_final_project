import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.JaasConfiguration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.awt.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;


public class TermSearch {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        String term = otherArgs[2];
        conf.set("term", args[2]);

        Job job = Job.getInstance(conf);
        job.setJobName("Term search");
        job.setJarByClass(TermSearch.class);
        job.setMapperClass(TopNMapper.class);
        job.setCombinerClass(TopNReducer.class);
        job.setReducerClass(TopNReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class TopNMapper extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private String tokens = "[_|$#<>\\^=\\[\\]\\* /\\\\,;,.\\-()?!\"']";

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String cleanLine = value.toString().toLowerCase().replaceAll(tokens, "");
            String[] removeTab = cleanLine.split("\t");
            //if(String.valueOf(removeTab[0]) == term) {
            StringTokenizer itr = new StringTokenizer(removeTab[0]);
            Configuration conf = context.getConfiguration();
            String term = conf.get("term");

            String[] numOccurences = removeTab[1].split(" ");
            int count = 0;
            if(String.valueOf(removeTab[0].trim()).equals(term.trim())) {

                for(String temp : numOccurences) {
                    context.write(new Text(term), new Text(temp.trim()));
                }

                //}

            }
        }
    }


    public static class TopNReducer extends Reducer<Text, Text, Text, Text> {


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text temp : values) {
                Configuration conf = context.getConfiguration();
                String term = conf.get("term");
                context.write(new Text(term), temp);
            }

        }




        public static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
            LinkedList<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

            Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

                public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });


            Map<K, V> sortedMap = new LinkedHashMap<K, V>();

            for (Map.Entry<K, V> entry : entries) {
                sortedMap.put(entry.getKey(), entry.getValue());
            }

            return sortedMap;
        }
    }
}




