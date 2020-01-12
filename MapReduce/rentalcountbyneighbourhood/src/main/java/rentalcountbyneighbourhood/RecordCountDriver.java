package rentalcountbyneighbourhood;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




import java.io.IOException;

public class RecordCountDriver
        extends Configured implements Tool{
    private  static  class RecordMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

        private Text word = new Text();
        public void map(LongWritable lineOffset, Text record, Context output)
        throws IOException, InterruptedException {
            String row = record.toString();
            String clean_row = row.replaceAll("[\\n\\t ]", "");
            String[] tokens = clean_row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");


            String col4 = tokens[4];


            if (!col4.contains("neighbourhood_group")) {
                word.set(col4);
                output.write(word,new LongWritable(1));
            }
            else{
                return;
            }

        }
    }

    public static class LongSumReducer extends
            Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);

        }
    }


    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        @SuppressWarnings("deprecation")
        Job job = new Job(getConf(), "Record Count");
        job.setJarByClass(getClass());

        job.setMapperClass(RecordMapper.class);
        job.setReducerClass(LongSumReducer.class);

        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);



        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int jobStatus = ToolRunner.run(new RecordCountDriver(), args);
        System.out.println(jobStatus);
    }
}