import java.util.Arrays;
import java.lang.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class profiling_mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
       private static final int MISSING = 9999;
        private final static IntWritable one = new IntWritable(1);
 @Override
        public void map(LongWritable key, Text lineText, Context context) throws IOException, InterruptedException {
String line = lineText.toString();
	String[] arrayString = line.split(",");
	if (arrayString.length>5) {
    String county_county = arrayString[arrayString.length-5];
    String state_name= arrayString[arrayString.length-1];
    String[] countyArr= county_county.split(" ");

	String[] arr2;
	if (countyArr[countyArr.length-1]=="County"){
 arr2 = Arrays.copyOfRange(countyArr,0,countyArr.length-1);
}
else {
 arr2 = Arrays.copyOfRange(countyArr,0,countyArr.length);
}	
String county = String.join(" ", arr2); 

    String joint= String.join(",",arrayString[1],arrayString[2],arrayString[3],arrayString[4],
    	arrayString[5],county,arrayString[arrayString.length-1]);
    context.write(new Text(joint), one);
}
}
}
