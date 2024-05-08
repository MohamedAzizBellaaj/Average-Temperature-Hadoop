package weather.batch.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgTempPerCapital {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "average temperature per capital");
        job.setJarByClass(AvgTempPerCapital.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(AvgTempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

// package weather.batch.project;

// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.FloatWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import com.mongodb.hadoop.MongoOutputFormat;
// import java.net.URLEncoder;
// import java.nio.charset.StandardCharsets;

// // ...

// public class AvgTempPerCapital {
// public static void main(String[] args) throws Exception {
// Configuration conf = new Configuration();

// // Set MongoDB output URI
// String encodedURL = URLEncoder.encode(
// "mongodb+srv://admin:1234@cluster0.iunwbji.mongodb.net/bigdata.weather_average",
// StandardCharsets.UTF_8.toString());

// conf.set("mongo.output.uri", encodedURL);
// Job job = Job.getInstance(conf, "average temperature per capital");
// job.setJarByClass(AvgTempPerCapital.class);
// job.setMapperClass(TokenizerMapper.class);
// job.setReducerClass(AvgTempReducer.class);
// job.setOutputKeyClass(Text.class);
// job.setOutputValueClass(FloatWritable.class);
// FileInputFormat.addInputPath(job, new Path(args[0]));

// // Set MongoDB as the output format
// job.setOutputFormatClass(MongoOutputFormat.class);

// System.exit(job.waitForCompletion(true) ? 0 : 1);
// }
// }
