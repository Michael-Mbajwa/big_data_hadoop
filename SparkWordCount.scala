import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.util.matching.Regex


object SparkWordCount {
  /*
A SparkWordCount.scala object, that extracts (i) words (i.e., sequences of the
letters “a-z” including dashes “-” and under-dashes “ ”) and (ii) numbers (i.e., sequences of the
digits “0-9” separated by at most one “.”) into two separate RDD objects.

It goes further to find the (i) top-1,000 most frequent words and the (ii) top-1,000 most
frequent numbers contained among provided file.
  */
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SparkWordCount")
    val ctx = new SparkContext(sparkConf)
    val textFile = ctx.textFile(args(0))
    
    val alphaRegex = """[a-z-_]+""".r
    val numRegex = """[-+]?[0-9]+[.]{0,1}[0-9]*""".r

    val origFile = textFile.flatMap(line => line.split(" "))
    val file = origFile.map(word => word.toLowerCase())
    
    val alphaFile = file.filter(x => (alphaRegex.pattern.matcher(x).matches))
    val numFile = file.filter(x => (numRegex.pattern.matcher(x).matches))

    val alphaCounts = alphaFile.map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2,false).take(1000)
    val numericCounts = numFile.map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2,false).take(1000)

    val alphaFinal = ctx.parallelize(alphaCounts, 2)
    val numFinal = ctx.parallelize(numericCounts, 2)

    alphaFinal.repartition(1).saveAsTextFile(args(1))
    numFinal.repartition(1).saveAsTextFile(args(2))
    ctx.stop()
  }
}
