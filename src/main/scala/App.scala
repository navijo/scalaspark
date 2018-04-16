import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class DataRow(name: String, value: Integer)

object App extends App {

  System.setProperty("hadoop.home.dir", "D:\\hadoop")

  val sparkConf = new SparkConf()
  sparkConf.setAppName("Testing")
  sparkConf.setMaster("local[1]")


  var spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  val spark2 = spark

  import spark2.implicits._

  val databaseTests = new Database()

  doThingsWithDataframes()
  doThingsWithDataSets()
  doWordCount()

  databaseTests.testKuduContext()
  databaseTests.testMongo()
  databaseTests.testKuduClient()

  def doThingsWithDataSets(): Unit = {
    println("\n\nDoing Things with Datasets")

    val dades = Seq("a", "b", "c", "d") zip (0 to 4)

    val ds = spark2.createDataset(dades)

    val ds2 = ds.map(row => DataRow(row._1, row._2))
    val ds3 = ds2.map(x => (x.name, x.value, x.value + 1))

    println("DataSet:\t" + ds3.collect().mkString(","))
    println("End of datasets things")
  }

  def doThingsWithDataframes(): Unit = {
   println("\n\nDoing Things with DataFrames")

    val data = Seq("a", "b", "c", "d") zip (0 to 4)

    val df = spark2.createDataFrame(data)

    val df2 = df.map(row => DataRow(row.getString(0), row.getInt(1)))
    val df3 = df2.map(x => (x.name, x.value, x.value + 1))

    println("DataFrame:\t" + df3.collect().mkString(","))
    println("End of dataframes things")
  }

  def doWordCount(): Unit = {
    println("\nDoing WordCount")

    val conf = com.typesafe.config.ConfigFactory.load()
    val filePath: String = conf.getString("files.path")

    val sc = SparkSession.builder().config(sparkConf).getOrCreate().sparkContext

    // split each document into words
    //val tokenized = sc.textFile("hdfs://quickstart.cloudera:8020/user/root/words.txt").flatMap(_.split("\\W+"))
    //val tokenized = sc.textFile("src/main/resources/words.txt").flatMap(_.split("\\W+"))
    val tokenized = sc.textFile(filePath).flatMap(_.split("\\W+"))

    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
    val wordCounts2 = tokenized.map((_, 1)).countByKey()

    // filter out words with less than threshold occurrences
    /*val threshold = 0
    val filtered = wordCounts.filter(_._2 >= threshold)*/

    val charFromWords = tokenized.map(s => s.length).reduce((a, b) => a + b)

    // count characters
    val chars = wordCounts.flatMap(_._1.toCharArray)
    val charCount = chars.map((_, 1)).reduceByKey(_ + _).sortByKey(ascending = true)


    println("Total Words: " + tokenized.count() + "\n" + wordCounts.collect().mkString(", "))
    println("Total Chars: " + charFromWords + "\n" + charCount.collect().mkString("\n"))
    println("Total Chars2: " + charFromWords + "\n" + wordCounts2.mkString("\n"))
    println("End of WordCount\n")
  }
}

