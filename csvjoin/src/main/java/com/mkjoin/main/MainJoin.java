package com.mkjoin.main;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.struct;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import javax.activation.DataSource;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.collection.JavaConversions;

import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.mkjoin.function.KafkaProducerVoidFunction;
import com.mkjoin.metadeta.MetaDataLoader;
import com.mkjoin.metadeta.model.Entity;

/**
 * 
 * {@link MainJoin} reads multiple csv files and join them on the bases of primary keys and foreign keys.
 * <br /> 
 * Primary keys and foreign keys defined in {@linkplain DataSource.json}. Relation between csv files are given in datasource.json
 * <br /> 
 * <h1>Supported joins: </h1>
 * 1. One To Many <br />
 * 2. One To One <br />
 * 
 * <h1>Command line arguments: </h1>
 * 	1. Boolean value (True for enabling Kafka) <br />
 *  2. Kafka Topic Name(If first is true) <br />
 *
 *{@link KafkaProducerVoidFunction} is producer, which works in best ways for producing data into topics.
 *
 * @author Mayank Rawat <br />
 *  
 */
public class MainJoin {

	public static void main(String[] args) throws JsonIOException, JsonSyntaxException, FileNotFoundException, AnalysisException {

		//Create Spark Config
		SparkConf conf = new SparkConf().setMaster("local[3]");
		SparkSession session = SparkSession.builder().config(conf).appName("Join Csv Files").getOrCreate();
		
		//Join CSV Data
		Dataset<String> joinedData = processData(MetaDataLoader.getParentEntity(), session).toJSON();
		
		
		boolean isKafkaEnable = Boolean.valueOf(args[0]);
		
		if(isKafkaEnable) {
			JavaSparkContext sc = new JavaSparkContext(session.sparkContext());
			//Kafka Sink
			joinedData.toJavaRDD().foreachPartition(new KafkaProducerVoidFunction(sc, args[1]));
			sc.close();
		} else {
			for (String str : joinedData.collectAsList()) {
				System.out.println(str);
			}
		}
	}

	/**
	 * Method will read data from csv and join them. 
	 * @param currentEntity is metadata 
	 * @param session
	 * @return joined dataset
	 */
	private static Dataset<Row> processData(Entity currentEntity, SparkSession session) {
		Dataset<Row> currentDataSet = session.read().option("header", "true").option("inferSchema", "true").csv(currentEntity.getFilePath());
		List<Column> groupByCols = new ArrayList<Column>();
		if (currentEntity.getParentEntity() != null) {
			for (String col : currentEntity.getFkColumns()) {
				groupByCols.add(new Column(col));
			}
		}
		List<Entity> childEntities = currentEntity.getChildEntities();
		Dataset<Row> dataset = currentDataSet;
		if (childEntities != null && childEntities.size() > 0) {
			for (Entity childEntity : childEntities) {
				dataset = dataset.join(processData(childEntity, session), JavaConversions.asScalaBuffer(currentEntity.getPrimaryKeys())
								.seq());
			}
			if (currentEntity.getParentEntity() == null)
				return dataset;
			
			return groupBy(currentEntity, dataset, groupByCols,
					dataset.columns());
		}

		// For Leaf Nodes
		if (currentEntity.getParentEntity() != null) {
			return groupBy(currentEntity, currentDataSet, groupByCols,
					currentDataSet.columns());
		}
		return currentDataSet;
	}

	/**
	 * Group by method is working like sql group by. Dataset will grouped on list of columns. 
	 * @param currentEntity is metadata
	 * @param dataSet to be groupby
	 * @param groupByCols columns using for groupby
	 * @param columnsArray 
	 * @return
	 */
	private static Dataset<Row> groupBy(Entity currentEntity,Dataset<Row> dataSet, List<Column> groupByCols, String[] columnsArray) {
		List<Column> columns = new ArrayList<Column>();
		for (String col : dataSet.columns()) {
			columns.add(dataSet.col(col));
		}
		Dataset<Row> grouped = null;
		if(currentEntity.getJoinType().equalsIgnoreCase("onetoone")) {
			grouped = dataSet.groupBy(JavaConversions.asScalaBuffer(groupByCols).seq()).agg(org.apache.spark.sql.functions.first(
					struct(JavaConversions.asScalaBuffer(columns).seq())).as(currentEntity.getEntityName() +""));
		} else{
			grouped = dataSet.groupBy(JavaConversions.asScalaBuffer(groupByCols).seq()).agg(collect_list(
						struct(JavaConversions.asScalaBuffer(columns).seq())).as(currentEntity.getEntityName() + "s"));
		}
		return grouped;
	}
}