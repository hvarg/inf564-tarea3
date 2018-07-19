package dsnn

object dsnn {
  //import breeze.linalg.{Vector,DenseVector}
  import org.apache.spark._
  import org.apache.spark.graphx.{Graph, Edge}

  def kMin (k: Int, a: Array[(Long, Int)], b: Array[(Long, Int)]): Array[(Long, Int)] = {
    var i = 0; var j = 0; var r = Array[(Long, Int)]()
    while (i < a.length || j < b.length) {
      if (j == b.length || (i != a.length && a(i)._2 < b(j)._2)) {
        r = r :+ a(i)
        i += 1
      } else {
        r = r :+ b(j)
        j += 1
      }
      if (r.length == k) { //break
        i = a.length; j = b.length;
      }
    }
    r
  }

  def main (args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dsnn").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val file = "bitcoinalpha.csv"
    //val file = "ezbit.csv"
    val k = 4
    println("--------------------------------------------")
    args.foreach(println)

    //Se lee el archivo y se transforma en un conjunto de arcos
    val lines = sc.textFile(file).map(line => line.split(','))
    val data = lines.map( arr => {
      Edge[Int](arr(0).toLong, arr(1).toLong, 10 - arr(2).toInt)
    })
    // Los arcos se transforman en un grafo.
    val graph = Graph.fromEdges[Int, Int](data, 0).cache()
    //println(graph.numEdges)
    //println(graph.numVertices)
    // Se calculan los k vecinos más cercanos a cada nodo.
    val kclosest = graph.aggregateMessages[Array[(Long, Int)]](
      triplet => { triplet.sendToSrc( Array((triplet.dstId, triplet.attr)) ) },
      (a,b) => { kMin(k, a, b) }
    )

    kclosest.foreach(x => println(x._1, x._2.deep))

    println("--------------------------------------------")
    sc.stop()
  }
}
