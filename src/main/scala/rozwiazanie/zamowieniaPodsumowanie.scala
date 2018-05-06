package rozwiazanie

import java.io.{File, FileNotFoundException}
import java.net.URL
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util._
import sys.process._

object zamowieniaPodsumowanie{

  val sparkMaster = ConfigFactory.load.getString("spark.master")

  val spark = SparkSession
    .builder()
    .master(sparkMaster)
    .appName("Zadanie stazowe - Allegro 2018")
    .getOrCreate()


  def fileDownloader(url: String, filename: String) = {
    print("Downloading file " + filename + "...")
    new URL(url) #> new File("src/main/resources/"+filename) !!

    println("done")
  }

  def downloadFiles(noOfFiles: Int): Unit = {
    val url = "http://data.europa.eu/euodp/repository/ec/dg-grow/mapps/TED_CN_"+(2007+noOfFiles-1).toString + ".csv"
    fileDownloader(url, "TED_CN_"+(2006+noOfFiles-1).toString + ".csv")
    if(noOfFiles > 1)
      downloadFiles(noOfFiles-1)
  }

  def getDataFromFile(path: String) : Try[DataFrame] = {
    try {
      val df : DataFrame = spark
        .read
        .option("header", "true")
        .csv(path)
      Success(df)
    } catch {
      case ex: FileNotFoundException => {
        println(s"""File $path not found""")
        Failure(ex)
      }
      case unknown: Exception => {
        println(s"""Unknown exception: $unknown""")
        Failure(unknown)
      }
    }
  }



  def main(args: Array[String]): Unit = {

    downloadFiles(2)
    getDataFromFile("src/main/resources/countries_code.csv") match {
      case Success(codesDf) => {
        codesDf.createOrReplaceTempView("CODES")

        getDataFromFile("src/main/resources/TED_CN_*") match {
          case Success(contractNotices) => {
            contractNotices
              .select("ISO_COUNTRY_CODE", "VALUE_EURO_FIN_2")
              .createOrReplaceTempView("CA")

            val filteredData = spark.sql(
              "SELECT * " +
                "FROM CA " +
                "JOIN CODES ON CA.ISO_COUNTRY_CODE = CODES.CODE " +
                "WHERE CA.VALUE_EURO_FIN_2 IS NOT NULL")
              .createOrReplaceTempView("FILTERED_DATA")

            val result = spark.sql(
              "SELECT ISO_COUNTRY_CODE AS COUNTRY, COUNT(*) AS COUNT, ROUND(AVG(VALUE_EURO_FIN_2), 2) AS AVERAGE " +
                "FROM FILTERED_DATA " +
                "GROUP BY ISO_COUNTRY_CODE " +
                "ORDER BY COUNT DESC").show(100)
          }
          case Failure(ex) => {
            println("Closing programm...")
          }
        }
      }
      case Failure(ex) => {
        println("Closing programm...")
      }
    }
    spark.close()
  }

}



//testy
//sciagac csv

/*
libraryDependencies += "com.softwaremill.sttp" %% "core" % "1.1.13"

libraryDependencies ++= Seq(

  "org.mockito" % "mockito-core" % "2.8.47" % "test",
  "com.googlecode.jmockit" % "jmockit" % "1.7" % "test",
  "junit" % "junit" % "4.8.1" % "test"
)

 */