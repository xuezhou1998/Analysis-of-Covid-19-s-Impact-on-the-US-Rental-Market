
import java.text.Format;
import java.util.*;
import java.util.Arrays;
import java.lang.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class predict_mapper extends Mapper<LongWritable, Text, Text, Text> {
       private static final int MISSING = 9999;
        private final static IntWritable one = new IntWritable(1);
 @Override
        public void map(LongWritable key, Text lineText, Context context) throws IOException, InterruptedException {
String inputText = lineText.toString();
        



                //String inputText="Wayne,IA:\t481,502,664,885,898,2019#519,543,715,889,970,2021#19185,89,3#465,489,650,856,885,2018#497,522,687,900,931,2020#";
        String state_county=inputText.toString().split(":")[0];
        String[] numArr = inputText.toString().split(":")[1].split("#");
       	
	if (numArr.length>=5){ 
	int[][] doubleintarr={{1,2},{3,4},{5,6}};
        int infection=0;
        int death=0;
        ArrayList<String[]> rents= new ArrayList<>();
        for (int i = 0; i < numArr.length; i++) {
            String[] currArr=numArr[i].split(",");
            if (currArr.length==3){
                 infection=Integer.valueOf(currArr[1]);
                 death=Integer.valueOf(currArr[2]);
            }
            else if (currArr.length==6){

                rents.add(currArr);

            }

        }

        double[] resultarrdb=predict(rents,infection,death);
	int[] resultarr= new int[5];
	for (int i =0;i< resultarrdb.length;i++){
		resultarr[i]=(int)resultarrdb[i];
	}
	String joint= String.join(",",state_county,String.valueOf(infection) ,String.valueOf(death),
            String.valueOf(resultarr[0]),
           String.valueOf(resultarr[1]) ,String.valueOf(resultarr[2]),String.valueOf(resultarr[3]),
         String.valueOf(resultarr[4]));
       // String joint= String.join(",",state_county,infection.toString(),death.toString(),resultarr[0].toString(),
         //   resultarr[1].toString(),resultarr[2].toString(),resultarr[3].toString(),
           // resultarr[4].toString());
        context.write(new Text(joint),new Text(""));
}
}
    public static double[] predict (ArrayList<String[]> k, int infections, int deaths){
        double[] resultarr= {0,0,0,0,0};

        double[] slope={0,0,0,0,0};
        double[] intercept={0,0,0,0,0};
        double[] sumxy={0,0,0,0,0};
        double[] sumx={0,0,0,0,0};
        double[] sumy={0,0,0,0,0};
        double[] sumxsquare={0,0,0,0,0};



        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 5; j++) {
                double x=Double.valueOf(k.get(i)[5]);
                double y=Double.valueOf(k.get(i)[j]);
                sumxy[j]+=x*y;
               sumx[j]+=x;
               sumy[j]+=y;
               sumxsquare[j]+=x*x;
            }
        }
        double N= ((double) k.size());
        for (int i = 0; i < 5; i++) {
            slope[i]=(N * sumxy[i]-sumx[i]*sumy[i])/(N*sumxsquare[i]-sumx[i]*sumx[i]);
            intercept[i]=(sumy[i]-slope[i]*sumx[i])/N;
        }

        for (int i = 0; i < resultarr.length; i++) {
            resultarr[i]=slope[i] * 2022.0 + intercept[i];
        }


        return resultarr;
    }
    public static double[] evaluate(ArrayList<String[]> k, int infections, int deaths){
        double[] results={0,0,0,0,0};

        return results;
    }
}
