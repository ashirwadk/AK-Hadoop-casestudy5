import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

  object Wordcount{

    def main(args: Array[String]): Unit = {
      println("hey Spark Streaming")

      val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSteamingExample")
      val sc = new SparkContext(conf)
      /*val rootLogger =Logger.getRootLogger()
      rootLogger.setLevel(Level.ERROR)*/
      val ssc = new StreamingContext(sc, Seconds(10))
      val lines = ssc.textFileStream("/home/acadgild")
      val words = lines.flatMap(_.split(" "))
      val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
      wordCounts.print()
      ssc.start()
      ssc.awaitTermination()


    }

  }
