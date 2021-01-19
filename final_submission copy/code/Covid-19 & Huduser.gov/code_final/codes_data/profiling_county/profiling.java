import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import java.lang.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class profiling {
        public static void main(String[] args) throws Exception {
//              Configuration conf = getConf();

//              conf.set("mapred.textoutputformat.separator", ","); 


                if (args.length != 2) {
                        System.err.println("Usage: Driver <in> <out>");
                        System.exit(2);
                }

                Job job = new Job();
                job.setJarByClass(profiling.class);
                job.setJobName("profiling");
                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                job.setMapperClass(profiling_mapper.class);
                job.setReducerClass(profiling_reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
