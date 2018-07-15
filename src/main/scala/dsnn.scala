package dsnn

object dsnn{
  import breeze.linalg.{Vector,DenseVector}
  import org.apache.spark._

  def main(args: Array[String]){
    val sparkConf = new SparkConf().setAppName("dsnn").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    var file = "/Users/hvargas/Downloads/rodrigo/bitcoinalpha.csv"
    val k = 10
    println("Hello word!")
    args.foreach(println)
    sc.stop()
  }
}
