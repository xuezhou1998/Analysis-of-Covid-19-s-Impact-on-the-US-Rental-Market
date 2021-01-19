
import java.util.*;


// import java.io.IOException;
// import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Reducer;
// public class evaluate_reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
public class evaluate_reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text inputText, Iterable<Text> values, Context context) throws IOException,
         InterruptedException {
String county_state= inputText.toString();


        int[] var2020 =new int[5] ;
        Arrays.fill(var2020,0);
        int[] var2021=new int[5];
        Arrays.fill(var2021,0);
        int[] var2019=new int[5];
        Arrays.fill(var2019,0);
        int infection_num=0;


        for (Text value: values) {
            //for loop wrapped
//            System.out.println(value[j]+"st");
            String[] inputarr=value.toString().split(",");

            if (inputarr.length==8){
//            county_state=String.join(",",inputarr[5],inputarr[6]);
//                System.out.println("x2021");
//                System.out.println(inputarr[7]+"xxxxx");
                if (inputarr[7].equals("2021")){
//                    System.out.println("x2021");
                    for (int i = 0; i <5 ; i++) {
                        var2021[i]=Integer.valueOf(inputarr[i]);

                    }

                }
                else if (inputarr[7].equals("2020")){
                    for (int i = 0; i <5 ; i++) {
                        var2020[i]=Integer.valueOf(inputarr[i]);
                    }
                }
                else if (inputarr[7].equals("2019")){
                    for (int i = 0; i <5 ; i++) {
                        var2019[i]=Integer.valueOf(inputarr[i]);
                    }
                }

            }
            else if (inputarr.length==9){
//            county_state=String.join(",",inputarr[0],inputarr[1]);
                infection_num= Integer.valueOf(inputarr[2]);

            }//for loop wrapped end
        }

        int[] var2020_2021 =new int[5] ;
        Arrays.fill(var2020_2021,0);
        int[] var2019_2020 =new int[5] ;
        Arrays.fill(var2019_2020,0);
        int[] varSlope_Change =new int[5] ;
        Arrays.fill(varSlope_Change,0);
        int[] resultarr =new int[5] ;
        Arrays.fill(resultarr,0);

        for (int i = 0; i <5 ; i++) {
            var2019_2020[i]=var2020[i]-var2019[i];
            var2020_2021[i]=var2021[i]-var2020[i];
            varSlope_Change[i]=var2020_2021[i]-var2019_2020[i];
//            System.out.println(String.valueOf(var2019_2020[i])+String.valueOf(var2020_2021[i])
//            +String.valueOf(varSlope_Change[i]));
            if (varSlope_Change[i]*infection_num<0){
                resultarr[i]=-1;
            }
            else if (varSlope_Change[i]*infection_num>0){
                resultarr[i]=1;
            }
            else {
                resultarr[i]=0;
            }
            // System.out.println(county_state+"#"+String.valueOf(resultarr[i])+"#"+
            //         String.valueOf(varSlope_Change[i]));
        }
	int summ=0;
        int average_slope_chge=0;
        for (int i = 0; i < 5; i++) {
            summ+=varSlope_Change[i];
            
        }
        average_slope_chge=summ/5;
        String jointresults=String.join(",",  String.valueOf(resultarr[0]),
                String.valueOf(resultarr[1]),String.valueOf(resultarr[2]),
                String.valueOf(resultarr[3]),String.valueOf(resultarr[4]),String.valueOf(infection_num),String.valueOf(average_slope_chge));
       Text county_state2=new Text(county_state.toString()+",");
                                 context.write(county_state2,  new Text(jointresults));
                                 }
                                 }
