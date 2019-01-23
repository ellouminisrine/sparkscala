
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{udf, lit}
import org.apache.spark.sql.functions._
import scala.util.Try
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf 
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.util.Calendar
import java.security.MessageDigest
import org.apache.spark.sql.functions.explode
import org.joda.time._
import sys.process._
import java.net.URL
import java.io.File
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object extraction {
 
  val makedatetype: (String=> String)= (date: String) => {
    try {
           date.slice(0,date.length -4)+"-"+ date.slice(6,date.length -2)+"-"+ date.slice(9,date.length -2) +" "+ date.slice(12,date.length)
        } catch {
               case e: Exception => "9999-99-99 99:99:99" 
               }
  }
  val calculdate = udf(makedatetype)
  
   val changemontant: (String => Float) = (valeur: String) => {
     try{ 
      valeur.slice(0,valeur.length -2).filter((!" ".contains(_))).toFloat
      } catch {
    case e: Exception => 0.0f 
      }
    }
   val montantToFloat = udf(changemontant)
  
    def main(args: Array[String]) = { 
    //Start the Spark context
  val conf = new SparkConf()
      .setAppName("lendoplis")
      .setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val df = sqlContext.read.json("lendopolis.json") 
  //schema for a malformed json file
  case class projets(name_file:String, Taux: String, nom_societe: String,date_debut_collecte: String, url: String, montant_collecte:String ,avancement: String,duree: String, rating_lendopolis:String)
// extraction des champs 
  val proj = df.select(explode($"projects").as("projects_flat")).toDF()
  val projApi =(proj.select("projects_flat.picture.picture.url","projects_flat.company.name","projects_flat.typology_name","projects_flat.funding_date","projects_flat.funding")
                    .withColumn("funding_date", calculdate(col("funding_date")))
                    .withColumn("funding",montantToFloat(col("funding")) ).toDF()
                    .toDF()
               )     

   //val som =projApi.columns.map(colName => sum(colName).as(s"$colName"))
  val  DFAgg =(projApi.groupBy("typology_name").agg(count("*")))           
  DFAgg.write.format("com.databricks.spark.csv").option("header", "false").mode("overwrite").save("outfile")
  //projApi.show()
  
  
  }
  
}