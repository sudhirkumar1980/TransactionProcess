

package TransactionProcess
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.types.LongType
import java.sql.Timestamp
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions._

object Launch {
def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("TransactionProcess")
      .getOrCreate()
      
   spark.sparkContext.setLogLevel("ERROR")
     
 val schema = StructType(
      List(
        StructField("First", StringType, true),
        StructField("Last", StringType, true),
        StructField("Email", StringType, true),
        StructField("Domain", StringType, true),
        StructField("IP", StringType, true),
        StructField("CreditCard", StringType, true),
        StructField("TransactionTime", LongType, true)
      )
    )      
    val df = spark.readStream.schema(schema).csv("./Resources/")
    

    val df_DateConverted = df.withColumn("TransactionTimeStamp", from_unixtime(df.col("TransactionTime")).cast("timestamp"));
    
      //Create record count in each sliding window of duration last 3 min sliding by every 2 min
     import spark.implicits._
     val windowcount= df_DateConverted.groupBy(window($"TransactionTimeStamp", "3 minutes", "2 minutes")).count()

    //Print output on cosole.
    windowcount
    .writeStream
      .format("console")
      .outputMode("complete")
      .option("checkpointLocation", "./checkpoint/")
      .option("truncate", "false")
      .start()
      .awaitTermination()    
}
  
}