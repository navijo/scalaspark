package main.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

case class DataRow(name: String, value: Integer)
case class Person(name: String, age: Long)

object App extends App {

  val sparkConf = new SparkConf()
  sparkConf.setMaster("local")
  sparkConf.setAppName("Testing")

  var spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  val spark2 = spark
  import spark2.implicits._

  doThingsWithDataframes()
  doThingsWithDataSets()
  doWordCount()

  def doThingsWithDataSets():Unit = {
    System.out.println("\n\nDoing Things with Datasets\n")

    val dades = Seq("a", "b", "c", "d") zip (0 to 4)

    val ds = spark2.createDataset(dades)

    val ds2 = ds.map(row => DataRow(row._1, row._2))
    val ds3 = ds2.map(x => (x.name, x.value, x.value + 1))

    System.out.println("DataSet:\t" + ds3.collect().mkString(","))
    System.out.println("End of datasets things\n")
  }

  def doThingsWithDataframes():Unit = {
    System.out.println("\n\nDoing Things with DataFrames\n")

    val data = Seq("a", "b", "c", "d") zip (0 to 4)

    val df = spark2.createDataFrame(data)

    val df2 = df.map(row => DataRow(row.getString(0),row.getInt(1)))
    val df3 = df2.map(x => (x.name, x.value, x.value + 1))

    System.out.println("DataFrame:\t" + df3.collect().mkString(","))
    System.out.println("End of dataframes things\n")
  }

  def doWordCount():Unit = {
    System.out.println("\nDoing WordCount")

    val conf = com.typesafe.config.ConfigFactory.load()
    val filePath : String = conf.getString("files.path")
    spark.close()
    spark2.close()
    val sc = new SparkContext(sparkConf)
    // split each document into words
    //val tokenized = sc.textFile("hdfs://quickstart.cloudera:8020/user/root/words.txt").flatMap(_.split("\\W+"))
    //val tokenized = sc.textFile("src/main/resources/words.txt").flatMap(_.split("\\W+"))
    val tokenized = sc.textFile(filePath).flatMap(_.split("\\W+"))

    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    // filter out words with less than threshold occurrences
    /*val threshold = 0
    val filtered = wordCounts.filter(_._2 >= threshold)*/

    val charFromWords = tokenized.map(s => s.length).reduce((a, b) => a + b)

    // count characters
    val chars = wordCounts.flatMap(_._1.toCharArray)
    val charCount = chars.map((_, 1)).reduceByKey(_ + _)

    System.out.println("Total Words: "+tokenized.count() +"\n" +wordCounts.collect().mkString(", "))
    System.out.println("Total Chars: "+charFromWords +"\n"+charCount.collect().mkString("\n"))
    System.out.println("End of WordCount\n\n")
  }
}

