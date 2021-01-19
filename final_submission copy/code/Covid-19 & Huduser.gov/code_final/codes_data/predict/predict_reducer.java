



// import java.io.IOException;
// import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Reducer;
// public class predict_reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//         @Override
//         public void reduce(Text inputText, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
// 			int sum  = 0;
//       // for ( IntWritable count  : counts) {
//       //   sum  += count.get();
//       // }
//         inputText=new Text(inputText.toString()+",");
//       context.write(inputText,  new IntWritable(sum));

// }
// }
import java.io.IOException;
import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class predict_reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text inputText, Iterable<Text> counts, Context context) throws IOException, InterruptedException {
                        // int sum  = 0;
      // for ( IntWritable count  : counts) {
      //       //   sum  += count.get();
      //             // }
                        // String inputarr= inputText.toString().split(",");

                           // inputText=new Text(inputText.toString());
                                 context.write(inputText,  new Text(""));
      //
                                 }
                                 }
