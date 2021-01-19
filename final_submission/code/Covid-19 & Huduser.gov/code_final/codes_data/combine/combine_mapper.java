import java.util.*;
import java.lang.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class combine_mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                
        String line=value.toString();

        String[] line_array=line.split(",");
        //System.out.println(line_array[0]);
        if (line_array[0].equals("2020-10-01")){
            String state_string ="Alabama,ALxxxxxx" +
                    "Alaska,AKxxxxxx" +
                    "Arizona,AZxxxxxx" +
                    "Arkansas,ARxxxxxx" +
                    "California,CAxxxxxx" +
                    "Colorado,COxxxxxx" +
                    "Connecticut,CTxxxxxx" +
                    "Delaware,DExxxxxx" +
                    "District of Columbia,DCxxxxxx" +
                    "Florida,FLxxxxxx" +
                    "Georgia,GAxxxxxx" +
                    "Hawaii,HIxxxxxx" +
                    "Idaho,IDxxxxxx" +
                    "Illinois,ILxxxxxx" +
                    "Indiana,INxxxxxx" +
                    "Iowa,IAxxxxxx" +
                    "Kansas,KSxxxxxx" +
                    "Kentucky,KYxxxxxx" +
                    "Louisiana,LAxxxxxx" +
                    "Maine,MExxxxxx" +
                    "Maryland,MDxxxxxx" +
                    "Massachusetts,MAxxxxxx" +
                    "Michigan,MIxxxxxx" +
                    "Minnesota,MNxxxxxx" +
                    "Mississippi,MSxxxxxx" +
                    "Missouri,MOxxxxxx" +
                    "Montana,MTxxxxxx" +
                    "Nebraska,NExxxxxx" +
                    "Nevada,NVxxxxxx" +
                    "New Hampshire,NHxxxxxx" +
                    "New Jersey,NJxxxxxx" +
                    "New Mexico,NMxxxxxx" +
                    "New York,NYxxxxxx" +
                    "North Carolina,NCxxxxxx" +
                    "North Dakota,NDxxxxxx" +
                    "Ohio,OHxxxxxx" +
                    "Oklahoma,OKxxxxxx" +
                    "Oregon,ORxxxxxx" +
                    "Pennsylvania,PAxxxxxx" +
                    "Rhode Island,RIxxxxxx" +
                    "South Carolina,SCxxxxxx" +
                    "South Dakota,SDxxxxxx" +
                    "Tennessee,TNxxxxxx" +
                    "Texas,TXxxxxxx" +
                    "Utah,UTxxxxxx" +
                    "Vermont,VTxxxxxx" +
                    "Virginia,VAxxxxxx" +
                    "Washington,WAxxxxxx" +
                    "West Virginia,WVxxxxxx" +
                   "Wisconsin,WIxxxxxx" +
                    "Wyoming,WYxxxxxx";
            String[] states_array= state_string.split("xxxxxx");
            Dictionary<String,String> mydict= new Hashtable();

            for (int i = 0; i<states_array.length;i++){
                String[] subarray=states_array[i].split(",");
                mydict.put(subarray[0],subarray[1]);
                //System.out.print(subarray[1]);
            }

            //System.out.print(String.format("%s", states_array[5]))
            String state_alpha= mydict.get(line_array[2].toString());
            String final_string = String.join(",",line_array[0],line_array[1],state_alpha,line_array[3],
                    line_array[4],line_array[5]);
            //System.out.println(final_string);
            context.write(new Text(final_string), one);
}
}
}


