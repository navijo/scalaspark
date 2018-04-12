import com.mongodb.casbah.Imports._
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.KuduPredicate.ComparisonOp
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.JavaConverters._


class Database {


  def testMongo(): Unit = {
    System.out.println("\n\nTesting MongoDB\n")
    val mongoClient = MongoClient("mongo.host", 27017)
    val db = mongoClient("test")
    val coll = db("test")
    val a = MongoDBObject("hello" -> "world")
    val b = MongoDBObject("language" -> "scala")
    coll.insert(a)
    coll.insert(b)
    coll.count()
    val allDocs = coll.find()
    println(allDocs)
    for (doc <- allDocs) println(doc)
    coll.remove(a)
    coll.remove(b)
    mongoClient.close()
    System.out.println("\n\nEnd Testing MongoDB\n")
  }

  def testKuduContext(): Unit = {
    System.out.println("\n\nTesting Kudu\n")

    val sqlContext = SparkSession.builder().getOrCreate().sqlContext

    // Read a table from Kudu
    val df = sqlContext.read.options(Map("kudu.master" -> "kudu.master:7051", "kudu.table" -> "impala::default.sfmta")).kudu

    // Query using the Spark API...
    df.select("*").filter("speed >= 5").show()


    // ...or register a temporary table and use SQL
    df.createOrReplaceTempView("test_table")
    val filteredDF = df.select("report_time", "vehicle_tag").filter("speed >= 5")

    // Use KuduContext to create, delete, or write to Kudu tables
    val kuduContext = new KuduContext("kudu.master:7051", sqlContext.sparkContext)

    // Create a new Kudu table from a dataframe schema
    // NB: No rows from the dataframe are inserted into the table
    if (kuduContext.tableExists("test_table")) {
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
    for (value <- df.take(10)) {
      System.out.println(value)
    }

    val alteredDF = df.withColumn("speed", col("speed") + 1)
    val tenUpdated = alteredDF.take(10)
    System.out.println("\n\nChanged Values:\n")
    for (value <- tenUpdated) {
      System.out.println(value)
    }

    kuduContext.updateRows(alteredDF, "test_table")

    val updatedDF = sqlContext.read.options(Map("kudu.master" -> "kudu.master:7051", "kudu.table" -> "test_table")).kudu

    /* System.out.println("\n\nUpdated Values:\n")
     for(valueBig<-updatedDF.collect()){
       for(valueSmall <- tenUpdated){
         if(valueBig.getLong(0) == valueSmall.getLong(0)
           && valueBig.getInt(1) == valueSmall.getInt(1)) {
           System.out.println(valueSmall)
         }
       }
     }*/


    // Data can also be inserted into the Kudu table using the data source, though the methods on KuduContext are preferred
    // NB: The default is to upsert rows; to perform standard inserts instead, set operation = insert in the options map
    // NB: Only mode Append is supported
    df.write.options(Map("kudu.master" -> "kudu.master:7051", "kudu.table" -> "test_table")).mode("append").kudu


    // Delete a Kudu table
    kuduContext.deleteTable("test_table")
    System.out.println("\n\nEnd Testing Kudu\n")
  }

  def testKuduClient(): Unit = {
    val kuduMaster = "kudu.master"

    val tableName = "kuduclientSample"

    val numRegistersToInsert = 100
    val numRangePartitions = 5

    println(" -- Starting ")
    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()

    try {
      try {
        println(" -- ")
        println("Creating the schema")
        val columnList = new java.util.ArrayList[ColumnSchema]()
        columnList.add(new ColumnSchemaBuilder("KEY_ID", Type.INT32).key(true).build())
        columnList.add(new ColumnSchemaBuilder("COL_D", Type.INT32).key(true).build())
        columnList.add(new ColumnSchemaBuilder("COL_A", Type.STRING).key(false).build())
        columnList.add(new ColumnSchemaBuilder("COL_B", Type.STRING).key(false).build())
        columnList.add(new ColumnSchemaBuilder("COL_C", Type.STRING).key(false).build())
        val schema = new Schema(columnList)


        val rangeKeys: java.util.List[String] = new java.util.ArrayList[String]()
        rangeKeys.add("KEY_ID")

        val hashKeys: java.util.List[String] = new java.util.ArrayList[String]()
        hashKeys.add("COL_D")


        if (kuduClient.tableExists(tableName)) {
          kuduClient.deleteTable(tableName)
        }


        println("Creating the table")

        val leftBoundRow: PartialRow = new PartialRow(schema)
        leftBoundRow.addInt(0, 0)
        val rightBoundRow: PartialRow = new PartialRow(schema)
        rightBoundRow.addInt(0, 2)

        kuduClient.createTable(tableName, schema,
          new CreateTableOptions().
            setRangePartitionColumns(rangeKeys).addRangePartition(leftBoundRow, rightBoundRow).
            addHashPartitions(hashKeys, 2))


        println("Creating the range partitions")
        for (x <- 2 until numRangePartitions * 2 by 2) {
          println("Creating the range partitions between " + x + " and " + (x + 2))
          val leftBoundRow: PartialRow = new PartialRow(schema)
          leftBoundRow.addInt(0, x)
          val rightBoundRow: PartialRow = new PartialRow(schema)
          rightBoundRow.addInt(0, x + 2)
          kuduClient.alterTable(tableName, new AlterTableOptions().addRangePartition(leftBoundRow, rightBoundRow))
        }

        println("Creating the unbounded range partitions")
        //If the row specifying the partition is empty, the partition will be unbounded
        val rightUnbounded: PartialRow = new PartialRow(schema)
        rightUnbounded.addInt(0, (numRangePartitions * 2) + 1)
        kuduClient.alterTable(tableName, new AlterTableOptions().addRangePartition(rightUnbounded, new PartialRow(schema)))

        val table = kuduClient.openTable(tableName)
        val session = kuduClient.newSession

        println("Inserting " + numRegistersToInsert + " registers...")
        val initTime = DateTime.now(DateTimeZone.UTC).getMillis
        for (x <- 0 until numRegistersToInsert by 1) {
          println(x)

          val insert: Insert = table.newInsert
          val row: PartialRow = insert.getRow

          row.addInt(0, x)
          row.addString(2, "value_1_" + x)
          row.addString(3, "value_2_" + x)
          row.addString(4, "value_2_" + x)
          if (x % 2 == 0) row.addInt(1, 0) else row.addInt(1, 1)

          session.apply(insert)
        }

        val endTime = DateTime.now(DateTimeZone.UTC).getMillis
        val diffTime = endTime - initTime
        println("Total time spent inserting:\t" + diffTime / 1000 + " seconds")


        println("Scanning the table by key")
        val projectColumns = new java.util.ArrayList[String](4)
        projectColumns.add("KEY_ID")
        projectColumns.add("COL_D")
        projectColumns.add("COL_A")
        projectColumns.add("COL_B")
        projectColumns.add("COL_C")

        val scanner: KuduScanner = kuduClient.newScannerBuilder(table)
          .setProjectedColumnNames(projectColumns)
          .build()

        while (scanner.hasMoreRows) {
          val results = scanner.nextRows
          while (results.hasNext) {
            val actualRow = results.next()
            println(actualRow.getInt(0) + "\t" + actualRow.getInt(1)
              + "\t" + actualRow.getString(2) + "\t" + actualRow.getString(3)
              + "\t" + actualRow.getString(4))

          }
        }

        println("Querying the table")

        val predicateScanner = kuduClient.newScannerBuilder(table).
          addPredicate(KuduPredicate.newComparisonPredicate(new ColumnSchemaBuilder("COL_D", Type.INT32).build()
            , ComparisonOp.EQUAL, 1)).build()

        while (predicateScanner.hasMoreRows) {
          val results = predicateScanner.nextRows
          while (results.hasNext) {
            val actualRow = results.next()
            println(actualRow.getInt(0) + "\t" + actualRow.getInt(1)
              + "\t" + actualRow.getString(2) + "\t" + actualRow.getString(3)
              + "\t" + actualRow.getString(4))
          }
        }

      }
    } finally {
      kuduClient.shutdown()
    }
    println("-- finished")
  }

}
