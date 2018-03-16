import main.scala.App.sparkConf
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import collection.JavaConverters._
import com.mongodb.casbah.Imports._

object Database extends App {

  //testKudu()
  testMongo()

  def testMongo() = {

    val mongoClient = MongoClient("192.168.56.102", 27017)
    val db = mongoClient("test")
    val coll = db("test")
    val a = MongoDBObject("hello" -> "world")
    val b = MongoDBObject("language" -> "scala")
    coll.insert( a )
    coll.insert( b )
    coll.count()
    val allDocs = coll.find()
    println( allDocs )
    for(doc <- allDocs) println( doc )
  }

  def testKudu() = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[4]")
    sparkConf.setAppName("Testing Database")

    val conf = new SparkConf().setAppName("Database Testing").setMaster("local")
    val sc = new SparkContext(conf)

    var spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val spark2 = spark

    val sqlContext = spark2

    // Read a table from Kudu
    val df = sqlContext.read.options(Map("kudu.master" -> "kudu.master:7051", "kudu.table" -> "impala::default.sfmta")).kudu

    // Query using the Spark API...
    df.select("speed").filter("speed >= 5").show()

    /*
  // ...or register a temporary table and use SQL
  df.createOrReplaceTempView("kudu_table")
  sqlContext.sql("select speed from sfmta where id >= 5").show()
  val filteredDF = sqlContext.sql("select speed from sfmta where id >= 5")

  // Use KuduContext to create, delete, or write to Kudu tables
  val kuduContext = new KuduContext("192.168.56.102:7051", sqlContext.sparkContext)

  // Create a new Kudu table from a dataframe schema
  // NB: No rows from the dataframe are inserted into the table
  kuduContext.createTable(
    "test_table", df.schema, Seq("key"),
    new CreateTableOptions()
      .setNumReplicas(1)
      .addHashPartitions(List("key").asJava, 3))

  // Insert data
  kuduContext.insertRows(df, "test_table")

  // Delete data
  //kuduContext.deleteRows(filteredDF, "test_table")
  kuduContext.deleteRows(filteredDF, "test_table")

  // Upsert data
  kuduContext.upsertRows(df, "test_table")

  // Update data
  val alteredDF = df.select("id", "count" + 1)
  kuduContext.updateRows(filteredDF, "test_table")

    // Data can also be inserted into the Kudu table using the data source, though the methods on KuduContext are preferred
    // NB: The default is to upsert rows; to perform standard inserts instead, set operation = insert in the options map
    // NB: Only mode Append is supported
    df.write.options(Map("kudu.master"-> "kudu.master:7051", "kudu.table"-> "test_table")).mode("append").kudu

  // Check for the existence of a Kudu table
  kuduContext.tableExists("another_table")

  // Delete a Kudu table
  kuduContext.deleteTable("unwanted_table")
  */
  }
}
