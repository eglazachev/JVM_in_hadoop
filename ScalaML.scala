import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object WordCount {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val spark = SparkSession.builder().getOrCreate()
//    val arr_to_string = udf((vs: Seq[String]) => vs match {
//      case null => null
//      case _    => s"""[${vs.mkString(",")}]"""
//    })
    import spark.sqlContext.implicits._
    spark.read
      .option("header", "true")
      .csv(input)
      .select(concat_ws("", $"text", $"label") as "docs")
//      .select(split($"docs","\\s") as "words")  //nothing of that is works correctly. I can't get correct "label" value for each row
//      .withColumn("words", arr_to_string($"words"))
      .select("docs").write.mode("overwrite").csv(output)
//    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
//    val tokenized = tokenizer.transform(df)
//    val remover = new StopWordsRemover()
//      .setInputCol("words")
//      .setOutputCol("filtered")
//    val removed = remover.transform(tokenized)
//    removed.select("filtered")
//      .withColumn("filtered", stringify(col("filtered")))
//      .select("filtered", "label")
//      .write.mode("overwrite").csv(output)
    spark.stop()
  }
}
