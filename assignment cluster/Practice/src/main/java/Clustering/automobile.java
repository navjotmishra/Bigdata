package Clustering;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;


public class automobile {
	
	public static void main(String[] args) {
		
		
		

	Logger.getLogger("org").setLevel(Level.ERROR);
	Logger.getLogger("akka").setLevel(Level.ERROR);
	
	    

	// Create a SparkSession with s3 credentials
	SparkSession spark = SparkSession.builder().config("spark.hadoop.fs.s3a.access.key", "AKIAIRAMZL2C6JZSO2RQ")
            .config("spark.hadoop.fs.s3a.secret.key", "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").appName("SaavnAnalytics")
            .master("local[*]")
            .getOrCreate();
	
	
//reading the data set from s3 location for user activity
   String s3path = "s3a://bigdataanalyticsupgrad/activity/sample100mb.csv";
   // String s3path = "data/sample.csv";
//loading it into dataset
    Dataset<Row> rawDataset = spark.read().option("header", "false").csv(s3path)
           .toDF("userId","timestamp","songId","date");
	Dataset<Row> datasetClea = rawDataset.na().drop();
//loading metadata from s3 and loading it into data set           
	//String s3path1 = "data/inputData/metadata/*";
	  String s3path1 = "s3a://bigdataanalyticsupgrad/newmetadata/*";

	Dataset<Row> rawDatasetMetaData = spark.read().option("header", "false").csv(s3path1).
			toDF("songId","artistId");
		//datasetCleanMetaData.show();
	
	Dataset<Row> datasetClean = datasetClea.withColumn("days_before", functions.datediff(
			functions.current_timestamp(),
			functions.unix_timestamp(rawDataset.col("date"), "yyyymmdd").cast("timestamp")))
			.drop("timestamp")
			.drop("date");
//finding the recency on days before column	
	Dataset<Row> clickStreamDataRecency = datasetClean.groupBy("userId", "songId")
			.agg(functions.min("days_before").alias("recency"));			
//clickStreamDataRecency.show();

//finding frequency 	
Dataset<Row> clickStreamDataFrequency = datasetClean.groupBy("userId", "songId")
				.agg(functions.count("*").alias("frequency"));
//clickStreamDataFrequency.show();
//join the frequency and recency and dropping not required columns	
Dataset<Row> dataset = clickStreamDataFrequency
.join(clickStreamDataRecency, 
			clickStreamDataRecency.col("userId").equalTo(clickStreamDataFrequency.col("userId"))
			.and(clickStreamDataRecency.col("songId").equalTo(clickStreamDataFrequency.col("songId"))),
			"inner")
.drop(clickStreamDataRecency.col("userId"))
.drop(clickStreamDataRecency.col("songId"));
	

//string indexer 	
	StringIndexer indexer2 = new StringIndexer().setInputCol("songId").setOutputCol("IND_songId");
	StringIndexerModel indModel2 = indexer2.fit(dataset);
	Dataset<Row> indexeddata2 = indModel2.transform(dataset);
	
//vector assembler to combine everything to features
	
		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[] {"IND_songId", "frequency", "recency"}).setOutputCol("features");
		Dataset<Row> finalData = assembler.transform(indexeddata2);

			//Model building
		
		BisectingKMeans BKmeans = new BisectingKMeans().setK(301);
		BisectingKMeansModel  model = BKmeans.fit(finalData);
		

		
			Dataset<Row> predictions = model.transform(finalData);
			//predictions.show(200);
						
			// Evaluate clustering by computing Silhouette score
			ClusteringEvaluator evaluator = new ClusteringEvaluator();
		
			double silhouette = evaluator.evaluate(predictions);
			System.out.println("Silhouette with squared euclidean distance = " + silhouette);
		
			
			// Shows the result
			Vector[] centers = model.clusterCenters();
			System.out.println("Cluster Centers: ");
			for (Vector center : centers) {	
				System.out.println(center);
			}

//group by cluster id's 			
		Dataset<Row> predictionCount = predictions.groupBy("prediction").agg(functions.count("*").alias("count"));
		
		//Dataset<Row> rawDatasetMetaData = spark.read().csv("data/inputData/metadata/*").
			//	toDF("songId","artistId");
		Dataset<Row> datasetCleanMetaData = rawDatasetMetaData.na().drop();
		Dataset<Row> datasetMf = predictions
				.join(datasetCleanMetaData, 
					predictions.col("songId").equalTo(datasetCleanMetaData.col("songId")), "inner")
				.drop(datasetCleanMetaData.col("songId"));
		//datasetMf.show();
		Dataset<Row> grouped = datasetMf.groupBy(datasetMf.col("prediction"), datasetMf.col("artistId"))
				.agg(functions.count("*").alias("user_count"));
		Dataset<Row> maxCountRows = grouped.groupBy(grouped.col("prediction"))
				.agg(functions.max("user_count").alias("max_user_count"));
		
		Dataset<Row> finalSet = grouped
				.join(maxCountRows, 
						grouped.col("prediction").equalTo(maxCountRows.col("prediction"))
							.and(grouped.col("user_count").equalTo(maxCountRows.col("max_user_count")))
						, "inner")												
				.drop(maxCountRows.col("prediction"))

				.drop(maxCountRows.col("max_user_count"));
		
//writing the intermediate 	result 
		
//finalSet.toJavaRDD().saveAsTextFile("data/output/");
		
finalSet.toJavaRDD().saveAsTextFile("/user/ec2-user/cluster-casestudy/outputs/");
//reading the data from S3 and loading into dataset
		
		Dataset<Row> rawNotificationClickDataset = spark.read().csv("s3a://bigdataanalyticsupgrad/notification_clicks").toDF("notification_id", "user_id", "date");
		//Dataset<Row> rawNotificationClickDataset = spark.read().csv("data/inputData/notification_clicks/*").toDF("notification_id", "user_id", "date");
		rawNotificationClickDataset = rawNotificationClickDataset.drop(rawNotificationClickDataset.col("date")).na().drop();			
		//reading the data from S3 and loading into dataset		
		//rawNotificationClickDataset.show();
		//System.out.println("three");
		Dataset<Row> notificationClickCount = rawNotificationClickDataset.groupBy("notification_id").agg(functions.count("*").alias("count"));
		System.out.println("No. of rows " + rawNotificationClickDataset.count());
		//notificationClickCount.show();

		// Loads data
		Dataset<Row> rawNotificationArtistDataset = spark.read().csv("s3a://bigdataanalyticsupgrad/notification_actor").toDF("notification_id", "artist_id");
		//Dataset<Row> rawNotificationArtistDataset = spark.read().csv("data/inputData/notification_actor/*").toDF("notification_id", "artist_id");
		rawNotificationArtistDataset = rawNotificationArtistDataset.na().drop(); 
		//rawNotificationArtistDataset.show();
		Dataset<Row> notificationCount = rawNotificationArtistDataset.groupBy("notification_id").agg(functions.count("*").alias("count"));

//calculating CTR		
		List<Row> notifications = notificationClickCount.join(rawNotificationArtistDataset, 
				rawNotificationArtistDataset.col("notification_id").equalTo(notificationClickCount.col("notification_id"))
						, "inner")
			 .select(notificationCount.col("notification_id")).distinct().takeAsList(5);			
//		List<Row> notifications = notificationCount.select("notification_id").takeAsList(5);
		for(Row notification : notifications) {
			String notificationId = notification.get(0).toString();

				List<Row> clickCountRows = notificationClickCount.filter(notificationClickCount.col("notification_id").equalTo(notificationId))
												.select("count").takeAsList(1);
				
				Long clickCount = clickCountRows.get(0).getLong(0);				
				
				
				List<Row> account_ids = rawNotificationArtistDataset.filter(rawNotificationArtistDataset.col("notification_id").equalTo(notificationId))
											.select("artist_id").collectAsList();
				
				long totalCluserWiseCount = 0L;
				List<Integer> visitedClusters = new ArrayList<Integer>();
				
				for(Row row : account_ids) {
					String accountId = row.getString(0);						
					List<Row> clusterIds = finalSet.filter(finalSet.col("artistId").equalTo(accountId)).select("prediction").collectAsList();
					
					for(Row clusterRow : clusterIds) {						
						int clusterId = clusterRow.getInt(0);
						
						if(visitedClusters.contains(clusterId)) 
							continue;
						
						visitedClusters.add(clusterId);
						
						
							List<Row> countRows = predictionCount.filter(predictionCount.col("prediction").
									equalTo(clusterId)).select("count").takeAsList(1);
						long	count = countRows.get(0).getLong(0);
						
						
						totalCluserWiseCount += count;
					}
				}
				if (totalCluserWiseCount != 0) {
					
					double ctr = (clickCount/totalCluserWiseCount);
				System.out.println("Notification: " + notificationId + "\n" + "ClickCount: " + clickCount +
									"\n" + "Total ClusterWise count: " + totalCluserWiseCount +
									"\n" + "Click Through ratio: " + ctr);
			
				}
				
		}
	spark.stop();
	}
}