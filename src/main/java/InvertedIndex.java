
//source (with some alterations) - https://github.com/imehrdadmahdavi/map-reduce-inverted-index/blob/master/InvertedIndex.java
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

//modified from WordCount code provided on 'Hadoop Project GCP Gudie'
public class InvertedIndex
{

    InvertedIndex(){}

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: Inverted Index <input path> <output path>");
            System.exit(-1);
        }
        //Creating a Hadoop job and assigning a job name for identification.
        Job job = new Job();
        job.setJarByClass(InvertedIndex.class);
        job.setJobName("Inverted Index");
        //The HDFS input and output directories to be fetched from the Dataproc job submission console.
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //Proividng the mapper and reducer class names.
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        //Setting the job object with the data types of output key(Text) and value(Text).
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }

    /*
    This is the Reducer class. It extends the Hadoop's Reducer class.
    This maps the intermediate key/value pairs we get from the mapper to a set
    of output key/value pairs, where the key is the word and value is docId:word's count.
    Here our input key is a Text and input value is a Text.
    And the ouput key is a Text and the value is a Text.
    */
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
    {
        /*
        Reduce method collects the output of the Mapper and combines individual counts into hashmap.
        */
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            HashMap<String, Integer> hm = new HashMap<String, Integer>();
            ArrayList<String> stop_words = new ArrayList<String> (Arrays.asList("a",
                    "an",
                    "and",
                    "but",
                    "is",
                    "or",
                    "the",
                    "to" ));
//default constructor
			/*
			Iterates through all the values available with a key and adds the value and its count to the
			hashmap and gives the final result as the key and its count in each documents (docId:word(key) count)
			*/
            String valueString = "";
            for (Text value : values)
            {
                valueString = value.toString();
                String valueStringShortened = valueString.substring(valueString.lastIndexOf("/") + 1);
                if(hm.containsKey(valueStringShortened)) {
                    if(!stop_words.contains(valueStringShortened)) {
                        int sum = hm.get(valueStringShortened);
                        sum += 1;

                        hm.put(valueStringShortened, new Integer(sum));
                    }
                } else {
                    hm.put(valueStringShortened, new Integer(1));
                }
            }
            StringBuilder sb = new StringBuilder("");

            //format hashmap values into docId:word count so it's easier to print/view
            for(String temp : hm.keySet()) {
                sb.append(temp + ":" + hm.get(temp) + " ");
            }

            context.write(key, new Text(sb.toString()));

        }
    }

    /*
    This is the Mapper class. It extends the Hadoop's Mapper class.
    This maps input key/value pairs to a set of intermediate (output) key/value pairs.
    Here our input key is a LongWritable and input value is a Text.
    And the output key is a Text and value is an Text.
    */
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        private Text word = new Text();
        Text docID = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {


            Set<String> stop_words = new HashSet<>(Arrays.asList("a","about","above","after","again","against","all","am","an","and","any","are","aren't","as","at","be","because","been","before","being","below","between","both","but","by","can't","cannot","could","couldn't","did","didn't","do","does","doesn't","doing","don't","down","during","each","few","for","from","further","had","hadn't","has","hasn't","have","haven't","having","he","he'd","he'll","he's","her","here","here's","hers","herself","him","himself","his","how","how's","i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","itself","let's","me","more","most","mustn't","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours	ourselves","out","over","own","same","shan't","she","she'd","she'll","she's","should","shouldn't","so","some","such","than","that","that's","the","their","theirs","them","themselves","then","there","there's","these","they","they'd","they'll","they're","they've","this","those","through","to","too","under","until","up","very","was","wasn't","we","we'd","we'll","we're","we've","were","weren't","what","what's","when","when's","where","where's","which","while","who","who's","whom","why","why's","with","won't","would","wouldn't","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves"));


//default constructor
            //Reading input one line at a time and tokenizing.
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            //Getting docID (filename)
            String docIDstring = ((FileSplit)context.getInputSplit()).getPath().toString();
            String docIDstringShort = docIDstring.substring(docIDstring.lastIndexOf("/") + 1);
            docID = new Text(docIDstringShort);

            //Iterating through all the words available in that line and forming the key value pair.
            while (tokenizer.hasMoreTokens())
            {
                //preprocess word to eliminate any nonalphabet chars and make lowercase
                //allows words like Hello and hello to be viewed as same words in reducer step
                String check = preprocess(tokenizer.nextToken());
                if(!stop_words.contains(check)) {
                    word.set(check);
                    //Sending to output collector(Context) which in-turn passes the output to Reducer.
                    context.write(word, docID);
                }
            }
        }

        //make string uniform
        public String preprocess(String str)
        {
            str = str.toLowerCase(); //make everything lowercase
            str = str.replaceAll("[^a-zA-Z]", ""); //remove any nonalphabet chars
            return str;
        }
    }
}