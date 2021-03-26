package binod.suman.spark;



import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

public class SparkKafkaConsumer {

	public static void main(String[] args) throws IOException {

		System.out.println("Spark Streaming started now .....");

		SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// batchDuration - The time interval at which streaming data will be divided into batches
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(20000));

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton("export");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		  List<String> allRecord = new ArrayList<String>();
		  final String COMMA = ",";

		  directKafkaStream.foreachRDD(rdd -> {

		  System.out.println("New data arrived  " + rdd.partitions().size() +" Partitions and " + rdd.count() + " Records");
			  if(rdd.count() > 0) {
				rdd.collect().forEach(rawRecord -> {

					  System.out.println(rawRecord);
					  System.out.println("***************************************");
					  System.out.println(rawRecord._2);
					  String record = rawRecord._2();
					  StringTokenizer st = new StringTokenizer(record,",");

					  StringBuilder sb = new StringBuilder();
					  while(st.hasMoreTokens()) {
						String step = st.nextToken();
						String type = st.nextToken();
						String amount = st.nextToken();
						String nameOrig = st.nextToken();
						String oldbalanceOrg = st.nextToken();
						String newbalanceOrig = st.nextToken();
						String nameDest = st.nextToken();
						String oldbalanceDest = st.nextToken();
						String newbalanceDest = st.nextToken();
						String isFraud = st.nextToken();
						String isFlaggedFraud = st.nextToken();
						sb.append(step).append(COMMA).append(type).append(COMMA).append(amount).append(COMMA).append(oldbalanceOrg).append(COMMA).append(newbalanceOrig).append(COMMA).append(oldbalanceDest).append(COMMA).append(newbalanceDest).append(COMMA).append(isFraud);
						allRecord.add(sb.toString());
					  }

				  });
				System.out.println("All records OUTER MOST :"+allRecord.size());
				FileWriter writer = new FileWriter("Master_dataset.csv");
				for(String s : allRecord) {
					writer.write(s);
					writer.write("\n");
				}
				System.out.println("Master dataset has been created : ");
				writer.close();
			  }
		  });

		ssc.start();
		ssc.awaitTermination();
	}


}



