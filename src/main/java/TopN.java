//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
//
//import java.io.IOException;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.HashMap;
//import java.util.LinkedHashMap;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.StringTokenizer;
//
//
////https://andreaiacono.blogspot.com/2014/03/mapreduce-for-top-n-items.html
//
//public class TopN {
//
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        String nval = otherArgs[2];
//        conf.set("nval", args[2]);
//        Job job = Job.getInstance(conf);
//        job.setJobName("Top N");
//        job.setJarByClass(TopN.class);
//        job.setMapperClass(TopNMapper.class);
//        job.setCombinerClass(TopNReducer.class);
//        job.setReducerClass(TopNReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setNumReduceTasks(1);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
//
//
//
//    public static class TopNMapper extends Mapper<Object, Text, Text, IntWritable> {
//
//        private final static IntWritable one = new IntWritable(1);
//        private Text word = new Text();
//        private String tokens = "[_|$#<>\\^=\\[\\]\\* /\\\\,;,.\\-()?!\"']";
//
//        @Override
//        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            String cleanLine = value.toString().toLowerCase().replaceAll(tokens, "");
//            String[] removeTab = cleanLine.split("\t");
//
//            String[] numOccurences = removeTab[1].split(" ");
//            for(String temp : numOccurences) {
//
//                String[] num = temp.split(":");
//
//                context.write(new Text(removeTab[0]), new IntWritable(Integer.parseInt(num[1])));
//            }
//        }
//    }
//
//    public static class TopNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//
//        private Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();
//
//        @Override
//        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//
//            // computes the number of occurrences of a single word
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//
//            // puts the number of occurrences of this word into the map.
//            // We need to create another Text object because the Text instance
//            // we receive is the same for all the words
//            countMap.put(new Text(key), new IntWritable(sum));
//        }
//
//        @Override
//        protected void cleanup(Context context) throws IOException, InterruptedException {
//
//            Map<Text, IntWritable> sortedMap = sortByValues(countMap);
//            Configuration conf = context.getConfiguration();
//            int nval = Integer.parseInt(conf.get("nval"));
//            int counter = 0;
//            for (Text key : sortedMap.keySet()) {
//                if (counter++ == nval) {
//                    break;
//                }
//                context.write(key, sortedMap.get(key));
//            }
//        }
//    }
//
//    public static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
//        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());
//
//        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
//
//            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
//                return o2.getValue().compareTo(o1.getValue());
//            }
//        });
//
//        //LinkedHashMap will keep the keys in the order they are inserted
//        //which is currently sorted on natural ordering
//        Map<K, V> sortedMap = new LinkedHashMap<K, V>();
//
//        for (Map.Entry<K, V> entry : entries) {
//            sortedMap.put(entry.getKey(), entry.getValue());
//        }
//
//        return sortedMap;
//    }
//
//
//    public static class TopNCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
//
//        @Override
//        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//
//            // computes the number of occurrences of a single word
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//            context.write(key, new IntWritable(sum));
//        }
//    }
//}
//
