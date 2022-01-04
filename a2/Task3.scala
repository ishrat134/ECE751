import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
import org.apache.spark.{SparkContext, SparkConf}

object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val output = textFile.flatMap(line =>{ line.split(",",-1).drop(1)
                          .zipWithIndex
                          .map{ case(element, index) => {
                          var count = 1
                          if(element.length()==0) count =0
                          (index, count)
                          } 
                          }}).reduceByKey(_+_).map{case (index, num) => (index+1) + "," + num}

    
    output.saveAsTextFile(args(1));
  }
  }

