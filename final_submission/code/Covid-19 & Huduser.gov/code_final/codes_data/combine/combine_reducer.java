import java.io.IOException;
import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class combine_reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text inputText, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                inputText=new Text(inputText.toString()+",");
                context.write(inputText,  new IntWritable(0));
        }
}