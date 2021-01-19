import java.lang.*;
import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class mapping_mapper extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text textinput, Context context) throws IOException, InterruptedException {
                
        // Object textinput="2020-10-01,Kankakee,IL,17091,2816,77, 0";
        //Object testinput2="1019,1229,1558,2072,2523,Miami-Dade,FL,2019";
    //     int branch;
    //     String county;
    //     String state;
    //     String year;
    //     String final_string;
    //     String[] text_arr=textinput.toString().split(",");
    //     if (text_arr[0].split("-")[0].equals("2020")){
    //         System.out.println(text_arr[0].split("-")[0].toString());
    //          county = text_arr[1];
    //          state= text_arr[2];
    //          branch=0;
    // }
    //     else {
    //         county=text_arr[5];
    //         state=text_arr[6];
    //         year= text_arr[7];
    //         branch=1;

    //     }  
    //     final_string= String.join(",", county, state);
    //     context.write(new Text(final_string), one);

            int branch;
        String county;
        String state;
        String year;
        String final_value;
        String final_key;
        String[] text_arr=textinput.toString().split(",");
        if (text_arr[0].split("-")[0].equals("2020")){

             county = text_arr[1];
             state= text_arr[2];
             branch=0;
             final_key=String.join(",", county,state);
             final_value=String.join(",",text_arr[3],text_arr[4],text_arr[5]);
    }
        else {
            county=text_arr[5];
            state=text_arr[6];
            year= text_arr[7];
            branch=1;
            final_key=String.join(",", county,state);
            final_value=String.join(",",text_arr[0],text_arr[1],text_arr[2],text_arr[3],text_arr[4],year);

        }
        context.write(new Text(final_key), new Text(final_value));

}
}
