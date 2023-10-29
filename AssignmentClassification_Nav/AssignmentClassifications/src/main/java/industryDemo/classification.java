package industryDemo;


import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class classification {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		SparkSession sparkSession = SparkSession.builder()  //SparkSession  
				.appName("SparkML") 
				.master("local[*]") 
				.getOrCreate(); //
		
	      
		// Read the file as a  dataset
		String path = "data/gender-classifier-DFE-791531.csv";	
		Dataset<Row> dataset = sparkSession.read().format("csv").option("header","true").option("mode", "DROPMALFORMED").load(path);
		
		Dataset<Row>[] splits = dataset.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> traindat = splits[0];		//Training Data
        Dataset<Row> testDat = splits[1];			//Testing Data
        
		//Isolate the relevant columns
		Dataset<Row> traindata = traindat.select(traindat.col("description"), 
				traindat.col("gender"),traindat.col("gender:confidence"),traindat.col("text")); 
		
			
        //ignoring the rows where gender it anything other than male,female or brand
		Dataset<Row> datasetCleantrain = traindata.where("gender = 'male' OR gender ='female' OR gender = 'brand' " );
        //Cleaning up the data with null values
		Dataset<Row> datasetCleantraindat = datasetCleantrain.na().drop();
		//concatenating description and text column
		Dataset<Row> datasetCleantrainda = datasetCleantraindat.withColumn("descriptio", concat(datasetCleantraindat.col("description"),
				lit(" "),datasetCleantraindat.col("text")));
		Dataset<Row> datasetCleantrai = datasetCleantrainda.select( 
				datasetCleantrainda.col("gender"),datasetCleantrainda.col("gender:confidence"),datasetCleantrainda.col("descriptio")); 
		//getting the values which only has gender confidence as 1
		Dataset<Row> datasetCleantraindata = datasetCleantrai.where(col("gender:confidence").$greater(.99999));
		

		//datasetCleantraindata.show();
		
		
		// testdata
		//Isolate the relevant columns
		Dataset<Row> testdata = testDat.select(testDat.col("description"), testDat.col("gender"),
				testDat.col("gender:confidence"),testDat.col("text")); 
		
		//ignoring the rows where gender it anything other than male,female or brand
		Dataset<Row> datasetCleantest = testdata.where("gender = 'male' OR gender ='female' OR gender = 'brand'");
		//Cleaning up the data with null values
		Dataset<Row> datasetCleantestda = datasetCleantest.na().drop();
		//concatenating description and text column
		Dataset<Row> datasetCleantes = datasetCleantestda.withColumn("descriptio", concat(datasetCleantestda.col("description"),
				lit(" "),datasetCleantestda.col("text")));
		Dataset<Row> datasetCleantestdat = datasetCleantes.select(datasetCleantes.col("gender"),
				datasetCleantes.col("gender:confidence"),datasetCleantes.col("descriptio")); 
		//getting the values which only has gender confidence as 1
		Dataset<Row> datasetCleantestdata = datasetCleantestdat.where(col("gender:confidence").$greater(.99999));
		//datasetCleantestdata.show();
		


		// Configure an ML pipeline, which consists of multiple stages: indexer, tokenizer, hashingTF, idf, lr/rf etc 
		// and labelindexer.		
		//Relabel the target variable
		StringIndexerModel labelindexer = new StringIndexer()
				.setInputCol("gender")
				.setOutputCol("label").fit(datasetCleantraindata);
		

		// Tokenize the input text
		Tokenizer tokenizer = new Tokenizer()
				.setInputCol("descriptio")
				.setOutputCol("words");

			

		// Create the Term Frequency Matrix
		HashingTF hashingTF = new HashingTF()
				.setNumFeatures(1000)
				.setInputCol(tokenizer.getOutputCol())
				.setOutputCol("numFeatures");

		// Calculate the Inverse Document Frequency 
		IDF idf = new IDF()
				.setInputCol(hashingTF.getOutputCol())
				.setOutputCol("features");

		// Set up the Random Forest Model
		RandomForestClassifier rf = new RandomForestClassifier();

		//Set up Decision Tree
		DecisionTreeClassifier dt = new DecisionTreeClassifier();

		// Convert indexed labels back to original labels once prediction is available	
		IndexToString labelConverter = new IndexToString()
				.setInputCol("prediction")
				.setOutputCol("predictedLabel").setLabels(labelindexer.labels());

		/*************************Model Building*********************/
		
		// Create and Run Random Forest Pipeline
		Pipeline pipelineRF = new Pipeline()
				.setStages(new PipelineStage[] {labelindexer, tokenizer, hashingTF, idf, rf,labelConverter});	
		// Fit the pipeline to training documents.
		PipelineModel modelRF = pipelineRF.fit(datasetCleantraindata);		
		// Make predictions on test documents.
		Dataset<Row> predictionsRF = modelRF.transform(datasetCleantestdata);
		System.out.println("Predictions from Random Forest Model are:");
		predictionsRF.show(10);

		// Create and Run Decision Tree Pipeline
		Pipeline pipelineDT = new Pipeline()
				.setStages(new PipelineStage[] {labelindexer, tokenizer,hashingTF, idf, dt,labelConverter});	
		// Fit the pipeline to training documents.
		PipelineModel modelDT = pipelineDT.fit(datasetCleantraindata);		
		// Make predictions on test documents.
		Dataset<Row> predictionsDT = modelDT.transform(datasetCleantestdata);
		System.out.println("Predictions from Decision Tree Model are:");
		predictionsDT.show(10);		

		// Select (prediction, true label) and compute test error.
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
				.setLabelCol("label")
				.setPredictionCol("prediction")
				.setMetricName("accuracy");		
	
	

		
		//Evaluate Random Forest
		double accuracyRF = evaluator.evaluate(predictionsRF);
		System.out.println("Accuracy for Random Forest = " + accuracyRF);
		

		//Evaluate Decision Tree
		double accuracyDT = evaluator.evaluate(predictionsDT);
		System.out.println("Accuracy for Decision Tree = " +accuracyDT);
	
		
		/*************************Model Evaluation*********************/
		
		// View confusion matrix
		System.out.println("Confusion Matrix for Random Forest:");
		predictionsRF.groupBy(col("label"), col("predictedLabel")).count().show();

		// Accuracy computation
		MulticlassClassificationEvaluator evaluator1 = new MulticlassClassificationEvaluator().setLabelCol("label")
				.setPredictionCol("prediction");
		double fscore = evaluator1.evaluate(predictionsRF);
		System.out.println("fscore for Random forest= " + fscore );
		
		// View confusion matrix
				System.out.println("Confusion Matrix for Decision Tree:");
				predictionsDT.groupBy(col("label"), col("predictedLabel")).count().show();

				// Accuracy computation
				MulticlassClassificationEvaluator evaluator2 = new MulticlassClassificationEvaluator().setLabelCol("label")
						.setPredictionCol("prediction");
				double fscore1 = evaluator2.evaluate(predictionsDT);
				System.out.println("fscore for Decision Tree= " + fscore1 );
	}

}
