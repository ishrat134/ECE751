import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer
// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))


    // modify this code
    val output = textFile.map(line => {
	val values  = line.split(",")
	val movie= values(0)
	val rating = values.slice(1,values.length)
	var maxRating = rating.max
	var users = ArrayBuffer[Int]()


	for (a<- 0 until rating.length){
	   if(maxRating == rating(a)){
	     users +=(a+1) 
	}	

	}

	movie+","+users.mkString(",")

});
    
    output.saveAsTextFile(args(1))
  }
}
