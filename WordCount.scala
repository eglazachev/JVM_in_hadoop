import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val sc = new SparkContext()
    sc.textFile(input)
      .flatMap(line => line.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().split("\\s"))
      .map(word => (word, 1))
      .reduceByKey(_ + _) // .reduceByKey((x, y) => x + y)
      .sortBy { case (word, count) => count } // .sortBy(_._2) or .sortBy(pair => pair._2)
      .map(_.swap)
      .saveAsTextFile(output)
    sc.stop()
  }
}