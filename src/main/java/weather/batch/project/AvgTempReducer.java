package weather.batch.project;

import org.bson.Document;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;

public class AvgTempReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();
    private MongoClient mongoClient;
    private MongoCollection<Document> collection;

    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;

        for (FloatWritable val : values) {
            sum += val.get();
            count++;
        }

        if (count > 0) {
            float avg = sum / count;
            result.set(avg);
            context.write(key, result);
        }

        // Create a new document or update an existing one with the average temperature
        Document filter = new Document("capital", key.toString());
        Document update = new Document("$set", new Document("average_temperature", result.get()));
        collection.updateOne(filter, update, new UpdateOptions().upsert(true));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // Connect to the MongoDB server and get the collection
        mongoClient = MongoClients.create("mongodb+srv://admin:1234@cluster0.iunwbji.mongodb.net");
        MongoDatabase database = mongoClient.getDatabase("bigdata");
        collection = database.getCollection("weather_average");
        // TODO: Add any additional setup code here
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        // Close the MongoDB connection
        mongoClient.close();
        // TODO: Add any additional cleanup code here
    }

}
