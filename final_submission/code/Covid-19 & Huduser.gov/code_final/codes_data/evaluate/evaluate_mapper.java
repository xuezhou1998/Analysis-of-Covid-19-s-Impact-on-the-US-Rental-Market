

import java.util.Arrays;
import java.lang.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class evaluate_mapper extends Mapper<LongWritable, Text, Text, Text> {
       private static final int MISSING = 9999;
        private final static IntWritable one = new IntWritable(1);
 @Override
        public void map(LongWritable key, Text lineText, Context context) throws IOException, InterruptedException {
            String[] inputarr=lineText.toString().split(",");
        String county_state="";
        if (inputarr.length==8){
             county_state=String.join(",",inputarr[5],inputarr[6]);

        }
        else if (inputarr.length==9){
            county_state=String.join(",",inputarr[0],inputarr[1]);
        }
        context.write(new Text(county_state),lineText);
}

}
