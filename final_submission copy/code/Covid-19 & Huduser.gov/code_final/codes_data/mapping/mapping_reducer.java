import java.io.IOException;
import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class mapping_reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> elements, Context context) throws IOException, InterruptedException {
                        
                          String final_string="";
                          for (Text element :elements ) {
                          final_string= final_string + element+"#";
                          }
                           key=new Text(key.toString()+":");

                                 context.write(key,  new Text(final_string));

                                 }
                                 }
