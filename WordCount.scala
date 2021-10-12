import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCount {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val stopWords = Seq("di", "yg")
    val spark = SparkSession.builder().getOrCreate()
    import spark.sqlContext.implicits._
    spark.read.option("header", "true").csv(input)
      .select(concat_ws(" ", $"class", $"comment") as "docs")
      .select(lower($"docs") as "docs")
      .select(regexp_replace($"docs","[^a-z0-9_ ]","") as "docs")
      .select(split($"docs",
        "\\s") as "words")
      .select(explode($"words") as "word").filter(!$"word".isin(stopWords:_*))
      .groupBy($"word").count().sort("count")
      .select($"count", $"word").write.mode("overwrite").csv(output)
    spark.stop()
  }
}