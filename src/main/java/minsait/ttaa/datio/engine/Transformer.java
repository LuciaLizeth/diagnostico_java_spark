package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
//import org.apache.spark.sql.expressions.Window;
//import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

//import minsait.ttaa.datio.common.naming.AgeRange;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private SparkSession spark;
     
    private static final int recordNumber = 50;
    private static final String rangeA = "A";
    private static final String rangeB = "B";
    private static final String rangeC = "C";
    private static final String rangeD = "D";
    
    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df.printSchema();

        df = fillAgeRange(df);
        df = fillNationalityPosition(df);
        df = fillPotentialVsOverall(df);
        
        df = addFilters(df);
        df = columnSelection(df);

        // for show 100 records after your transformations and show the Dataset schema
        df.show(recordNumber , false);
        df.printSchema();

        // Uncomment when you want write your final output
        //write(df);
    }

    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
        		//1. 
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                //2. new column
                ageRange.column(),
                //3. new column
                nationalityPosition.column(),
                //4. new column
                potentialVsOverall.column()
        );
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> addFilters(Dataset<Row> df) {
        df = df.filter( teamPosition.column().isNotNull()
                .and( shortName.column().isNotNull() )
                .and( overall.column().isNotNull() )
                .and( nationalityPosition.column().isNotNull() )
                .or(nationalityPositionLessThan(3)) //5. New Filters Added
                .or((col(ageRange.getName()).isin(rangeB, rangeC)).and(potentialVsOveralGreaterThan(1.15)) )
                .or( ageRangeEqualTo(rangeA).and(potentialVsOveralGreaterThan(1.25)) )
                .or( ageRangeEqualTo(rangeD).and(nationalityPositionLessThan(5)) )
        );

        return df;
    }
    
    private Column potentialVsOveralGreaterThan(double value) {
    	return greaterThan(potentialVsOverall.getName(),value);
    }
    
    private Column ageRangeEqualTo(String value) {
    	return col(ageRange.getName()).equalTo(value);
    } 
    
    private Column nationalityPositionLessThan(double value) {
    	return lessThan(nationalityPosition.getName(),value);
    } 

    private Column ageLessEqualsThan(int value) {
    	return lessEqualsThan(age.getName(),value);
    } 
    
    private Column ageGreaterEqualsThan(int value) {
    	return greaterEqualsThan(age.getName(), value);
    } 
    
    private Column lessThan(String columName, double value) {
    	return col(columName).$less(value);
    } 

    private Column greaterThan(String columName, double value) {
    	return col(columName).$greater(value);
    } 
    
    private Column lessEqualsThan(String columName, int value) {
    	return col(columName).$less$eq(value);
    } 
    
    private Column greaterEqualsThan(String columName, int value) {
    	return col(columName).$greater$eq(value);
    } 
    
    //2. New field "age_range" 
    private Dataset<Row> fillAgeRange(Dataset<Row> df) {

        Column ageRangeRule = when(ageLessEqualsThan(22), rangeA)
        		.when(ageLessEqualsThan(26), rangeB)
        		.when(ageLessEqualsThan(31), rangeC)
        		.when(ageGreaterEqualsThan(32), rangeD);
        
        df = df.withColumn(ageRange.getName(), ageRangeRule);
        return df;
    }
    
   //3. New field "nationality position" 
    private Dataset<Row> fillNationalityPosition(Dataset<Row> df) {
    	WindowSpec w = Window
                .partitionBy(
                		nationality.column(), 
                		teamPosition.column())
                .orderBy(overall.column().desc());

        Column nationalityRule = row_number().over(w);
        df = df.withColumn(nationalityPosition.getName(), nationalityRule);
        return df;
    }

    private Column divide( Column dividend, Column divisor) {
    	return dividend.divide(divisor);
    }
    
	//4. New field "potential_vs_overall" 
    private Dataset<Row> fillPotentialVsOverall (Dataset<Row> df) {
        
        df = df.withColumn(potentialVsOverall.getName(), divide(col(potential.getName()), col(overall.getName())));
        return df;
    }

}
