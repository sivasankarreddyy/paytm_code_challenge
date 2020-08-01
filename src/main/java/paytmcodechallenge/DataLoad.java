package paytmcodechallenge;

import java.net.URL;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

public class DataLoad {

	public static final String COLUMN_TEMP_STRING_TO_DOUBLE = "columnStringToDouble";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkSession spark = SparkSession.builder()
	     .master("local")
	     .appName("Word Count")
	    .getOrCreate();
		
		spark.sqlContext().udf().register(COLUMN_TEMP_STRING_TO_DOUBLE, (UDF1<String, Double>)
			    (columnValue) -> {
			return Double.parseDouble(columnValue.replace("*", ""));
			}, DataTypes.DoubleType);
		
		Dataset<Row> countryListDF=spark.read()
				  .option("header", "true")
				  .option("inferSchema", "true")
				  .csv(DataLoad.class.getResource("countrylist.csv").toString());
		
		Dataset<Row> stationListDF=spark.read()
				  .option("header", "true")
				  .option("inferSchema", "true")
				  .csv(DataLoad.class.getResource("stationlist.csv").toString());
		
		Dataset<Row> countryStationMappingDF = countryListDF.join(stationListDF, countryListDF.col("COUNTRY_ABBR").equalTo(stationListDF.col("COUNTRY_ABBR")))
				.drop(countryListDF.col("COUNTRY_ABBR"))
				.drop(stationListDF.col("COUNTRY_ABBR"));
	
		
		Dataset<Row> globalWeatherDataDF = loadCsvData("part-00000-0cdcbf51-ce47-471c-9877-65b068aa1670-c000.csv.gz", spark)
				.union(loadCsvData("part-00001-0cdcbf51-ce47-471c-9877-65b068aa1670-c000.csv.gz", spark))
				.union(loadCsvData("part-00002-0cdcbf51-ce47-471c-9877-65b068aa1670-c000.csv.gz", spark))
				.union(loadCsvData("part-00003-0cdcbf51-ce47-471c-9877-65b068aa1670-c000.csv.gz", spark))
				.union(loadCsvData("part-00004-0cdcbf51-ce47-471c-9877-65b068aa1670-c000.csv.gz", spark));
		
		
		Dataset<Row> weatherWithCountryFullNameDF = globalWeatherDataDF.join(countryStationMappingDF, globalWeatherDataDF.col("STN---").equalTo(countryStationMappingDF.col("STN_NO")));
		
		//QUESTION 1 : Which country had the hottest mean temperature?
		Dataset<Row> hotMeanTempDF = weatherWithCountryFullNameDF.withColumn("MAX", callUDF(COLUMN_TEMP_STRING_TO_DOUBLE, weatherWithCountryFullNameDF.col("MAX")))
				.withColumn("MIN", callUDF(COLUMN_TEMP_STRING_TO_DOUBLE, weatherWithCountryFullNameDF.col("MIN"))).cache();
		
		//hotMeanTempDF.show(false);
		
		Dataset<Row> hotMeanTempCountryDF = maxMeanHotTemp(hotMeanTempDF);
		
		
		Dataset<Row> coldMeanTempCountryDF = minMeanColdTemp(hotMeanTempDF);
	
		
		Dataset<Row> secondMaxMeanWindSpeed = secondMaxMeanWindSpeed(hotMeanTempDF);
			
		Row hotMeanTempCountryRow = hotMeanTempCountryDF.head();
		Row coldMeanTempCountryRow = coldMeanTempCountryDF.head();
		Row secondMaxMeanWindSpeedRow = secondMaxMeanWindSpeed.head();
		
		System.out.println("hot country is : " + hotMeanTempCountryRow.apply(0) + " with temperature : " + hotMeanTempCountryRow.apply(1));
		System.out.println("cold country is : " + coldMeanTempCountryRow.apply(0) + " with temperature : " + coldMeanTempCountryRow.apply(1));
		System.out.println("second highest wind speed country is : " + secondMaxMeanWindSpeedRow.apply(0) + " with wind speed : " + secondMaxMeanWindSpeedRow.apply(1));
		
	     
	}
	
	public static Dataset<Row> secondMaxMeanWindSpeed(Dataset<Row> df){

		WindowSpec windowSpec = Window.orderBy(desc("maxMeanWindSpeed"));
				
		Dataset<Row> maxMeanWindSpeed = df.groupBy("COUNTRY_FULL").agg(avg(col("WDSP")).alias("maxMeanWindSpeed"));

		Dataset<Row> maxMeanWindSpeedWithRN = maxMeanWindSpeed.withColumn("rn", row_number().over(windowSpec));
		
		return maxMeanWindSpeedWithRN.filter(col("rn").equalTo(2));
	}
	
	public static Dataset<Row> maxMeanHotTemp(Dataset<Row> df){

		Dataset<Row> avgHotMeanTempDF = df.groupBy("COUNTRY_FULL").agg(avg(col("MAX")).alias("maxMeanTemp"));

	
		return avgHotMeanTempDF.orderBy(desc("maxMeanTemp"));
	}
	
	public static Dataset<Row> minMeanColdTemp(Dataset<Row> df){
		
		Dataset<Row> avgColdMeanTempDF = df.groupBy("COUNTRY_FULL").agg(avg(col("MIN")).alias("minMeanTemp"));
		
		return avgColdMeanTempDF.orderBy(asc("minMeanTemp"));
	}
	
	
	public static Dataset<Row> loadCsvData(String fileName, SparkSession spark){
		
		return spark.sqlContext().read().format("csv")
		.option("header", "true").load(DataLoad.class.getResource(fileName).toString());
		
	}
	

}
