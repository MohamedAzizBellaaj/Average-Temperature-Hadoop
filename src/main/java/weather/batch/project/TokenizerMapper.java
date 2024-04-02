package weather.batch.project;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {

    private Text countryAndCapital = new Text();
    private FloatWritable temperature = new FloatWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");

        if (tokens.length >= 4) { // Ensure we have at least three columns
            String country = tokens[0];
            String capitalName = tokens[1]; // Assuming capital is the second column
            String temp = tokens[4]; // Assuming avg_temp_c is the fifth column
            
            try {
                float tempValue = Float.parseFloat(temp);
                String countryAndCapitalName = country + " - " + capitalName;
                countryAndCapital.set(countryAndCapitalName);
                temperature.set(tempValue);
                context.write(countryAndCapital, temperature);
            } catch (NumberFormatException e) {
                // Handle parsing errors
            }
        }
    }
}
