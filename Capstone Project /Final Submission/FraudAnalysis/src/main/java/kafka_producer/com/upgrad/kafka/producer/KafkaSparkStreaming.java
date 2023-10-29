package kafka_producer.com.upgrad.kafka.producer;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import kafka_producer.com.upgrad.kafka.producer.HbaseDAO;
import kafka_producer.com.upgrad.kafka.producer.TranData;
import kafka_producer.com.upgrad.kafka.producer.CardAnalysis;
import com.fasterxml.jackson.databind.ObjectMapper;
public class KafkaSparkStreaming {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkStreamingDemo").setMaster("local");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "100.24.223.181:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "upgraduserkafkasparknavjot3333"); //change here
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", true);
        Collection<String> topics = Arrays.asList("transactions-topic-verified");
       
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
        JavaDStream<String> jds = stream.map(x -> x.value()); 
       
       // To find the no of transactions from the incoming kafka streams
        
       jds.foreachRDD(x -> System.out.println("Total streams:"+x.count()));
       //process only the valid records from the stream
      JavaDStream<String> jdsResult = jds.filter(y -> (y!=null && y.toString().startsWith("{") && y.toString().endsWith("}")));
            
      JavaDStream<String> jdsResult1 = jdsResult.map(new Function<String,String>(){
   	   private static final long serialVersionUID = 1L;
   	   @Override
          public String call(String t) throws IOException, NumberFormatException, ParseException {
   		   ObjectMapper maper = new ObjectMapper();
   		   TranData transactionData = maper.readValue(t, TranData.class);
   		  PrintWriter writer = new PrintWriter(new File("data/test.csv")) ;
   	       String status=null;
   		   CardAnalysis cardStats = HbaseDAO.getStats(transactionData.getCard_id());
   		   if(cardStats != null && cardStats.checkUCL(transactionData.getAmount())
   				   && cardStats.checkScore() && cardStats.checkPostCode(transactionData.getPostcode(), transactionData.getTransaction_dt()))
   		   {
   			// Transaction validated successfully on all 3 rules
			   status="GENUINE";		 
			}
			else
			{
				// fraud transaction
				status="FRAUD";
			}
   		    System.out.println(transactionData.toString()+","+status);  		  
   		writer.close();
			return transactionData.toString()+status;
   	   }
   	   }
	

);
      
      // To find the no of transactions after filtering junk and null records
      jdsResult1.foreachRDD(x -> System.out.println("Total Transactions:"+x.count()));

      
       jssc.start();
       // Add Await Termination to respond to Ctrl+C and gracefully close Spark
       jssc.awaitTermination();
    
        }
    }
