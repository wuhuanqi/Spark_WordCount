import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  System.setProperty ("hadoop.home.dir", "D:\\QQfile\\hadoop-2.7.4\\hadoop-2.7.4")
  def main(args: Array[String]) {
    var masterUrl   = "local[1]"
    var inputPath = "C:\\Users\\user\\Documents\\spark\\data.txt"
    var outputPath = "C:\\Users\\user\\Documents\\spark\\output"

    if (args.length == 1) {
      masterUrl = args(0)
    } else if (args.length == 3) {
      masterUrl = args(0)
      inputPath = args(1)
      outputPath = args(2)
    }

    println(s"masterUrl:${masterUrl}, inputPath: ${inputPath}, outputPath: ${outputPath}")

    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rowRdd = sc.textFile(inputPath)
    val resultRdd = rowRdd.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1)).reduceByKey(_ + _)

    resultRdd.saveAsTextFile(outputPath)
    Thread.sleep(100000)
  }

}

