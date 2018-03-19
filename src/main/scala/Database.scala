import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._
import org.apache.spark.sql.{SparkSession}
import collection.JavaConverters._
import com.mongodb.casbah.Imports._
import org.apache.spark.sql.functions._

class Database{


  def testMongo() = {
    System.out.println("\n\nTesting MongoDB\n")
    val mongoClient = MongoClient("mongo.host", 27017)
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
    coll.remove(a);
    coll.remove(b);
    System.out.println("\n\nEnd Testing MongoDB\n")
  }

  def testKudu() = {
    System.out.println("\n\nTesting Kudu\n")

    var spark: SparkSession = SparkSession.builder().getOrCreate()
    val spark2 = spark

    val sqlContext = spark2

    // Read a table from Kudu
    val df = sqlContext.read.options(Map("kudu.master" -> "kudu.master:7051", "kudu.table" -> "impala::default.sfmta")).kudu

    // Query using the Spark API...
    df.select("*").filter("speed >= 5").show()


    // ...or register a temporary table and use SQL
    df.createOrReplaceTempView("test_table")
    val filteredDF = df.select("report_time","vehicle_tag").filter("speed >= 5")

    // Use KuduContext to create, delete, or write to Kudu tables
    val kuduContext = new KuduContext("kudu.master:7051", sqlContext.sparkContext)

    // Create a new Kudu table from a dataframe schema
    // NB: No rows from the dataframe are inserted into the table
    if(kuduContext.tableExists("test_table")) {
      kuduContext.deleteTable("test_table")
    }
    kuduContext.createTable(
      "test_table", df.schema, Seq("report_time", "vehicle_tag"),
      new CreateTableOptions()
        .setNumReplicas(1)
        .addHashPartitions(List("report_time", "vehicle_tag").asJava, 3))


    // Insert data
    kuduContext.insertRows(df, "test_table")

    // Delete data
    kuduContext.deleteRows(filteredDF, "test_table")

    // Upsert data
    kuduContext.upsertRows(df, "test_table")

    // Update data
    System.out.println("\n\nOriginal Values:\n")
    for(value <- df.take(10)){
      System.out.println(value)
    }

    val alteredDF = df.withColumn("speed",col("speed")+1)
    val tenUpdated =  alteredDF.take(10)
    System.out.println("\n\nChanged Values:\n")
    for(value<-tenUpdated){
      System.out.println(value)
    }

    kuduContext.updateRows(alteredDF, "test_table")

    val updatedDF = sqlContext.read.options(Map("kudu.master" -> "kudu.master:7051", "kudu.table" -> "test_table")).kudu

    System.out.println("\n\nUpdated Values:\n")
    for(valueBig<-updatedDF.collect()){
      for(valueSmall <- tenUpdated){
        if(valueBig.getLong(0) == valueSmall.getLong(0)
          && valueBig.getInt(1) == valueSmall.getInt(1)) {
          System.out.println(valueSmall)
        }
      }
    }


    // Data can also be inserted into the Kudu table using the data source, though the methods on KuduContext are preferred
    // NB: The default is to upsert rows; to perform standard inserts instead, set operation = insert in the options map
    // NB: Only mode Append is supported
    df.write.options(Map("kudu.master"-> "kudu.master:7051", "kudu.table"-> "test_table")).mode("append").kudu


    // Delete a Kudu table
    kuduContext.deleteTable("test_table")
    System.out.println("\n\nEnd Testing Kudu\n")
  }
}
