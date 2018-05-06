package solution

import java.io.{File, PrintWriter}

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite, Outcome}

class ordersSummaryTest extends FunSuite with BeforeAndAfterEach {

  var sparkSession : SparkSession = _
  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("Tests")
      .master("local")
      .getOrCreate()
  }

  override def afterEach() {
    sparkSession.stop()
  }

  test("testGetDataFromFileSuccess") {
    val file = new File("test.csv")
    val pw = new  PrintWriter(file)
    pw.write("col1,col2,col3\nt1,t2,t3")
    pw.close()

    val testDf = ordersSummary.getDataFromFile("test.csv")
    assert(testDf.isSuccess)

    file.delete()
  }

  test("testGetDataFromFileFail") {
    val testDf = ordersSummary.getDataFromFile("test.csv")
    assert(testDf.isFailure)
  }



}
