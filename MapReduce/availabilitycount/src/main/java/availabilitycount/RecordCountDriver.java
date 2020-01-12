package availabilitycount;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




import java.io.IOException;

public class RecordCountDriver
        extends Configured implements Tool{
    private  static  class RecordMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        public void map(LongWritable lineOffset, Text record, Context output)
        throws IOException, InterruptedException {
            String row = record.toString();
            String clean_row = row.replaceAll("[\\n\\t ]", "");
            String[] tokens = clean_row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            int length = tokens.length;
            String col15 = tokens[length - 1];


            if (!col15.contains("availability_365")) {


                int colInt15 = Integer.parseInt(col15);


                if (colInt15 == 365) {
                    // context.write(new IntWritable(colInt3), new IntWritable(colInt0));
                    output.write(new Text("Count"),new LongWritable(1));
                }
                else{
                    return;
                }
            }
            else{
                return;
            }

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