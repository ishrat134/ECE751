import org.apache.spark.{SparkConf, SparkContext}

object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val records = sc.broadcast(textFile.map(line => {
      val tokens = line.split(",")
      val rating = new Array[Byte](tokens.length - 1)
      for (i <- 1 until tokens.length) {
        if (tokens(i).length() > 0) {
          rating(i - 1) = tokens(i).toByte
        } else {
          rating(i - 1) = 0
        }
      }
      (tokens(0), rating)
    }).collectAsMap())

    val output1 = textFile.flatMap(line => {
      val tokens = line.split(",", 2)
      val m1 = tokens(0)
      val r1 = records.value(m1)
      records.value
        .filterKeys(m2 => m2.compareTo(m1) > 0)
        .map(pair => {
          val m2 = pair._1
          val r2 = pair._2
          var count = 0
          val num = Math.min(r1.length, r2.length)
          for (i <- 0 until num) {
            if (r1(i) == r2(i) && r1(i) != 0) count += 1
          }
          m1 + "," + m2 + "," + count
        })
    })

    output1.saveAsTextFile(args(1))
 }
}
