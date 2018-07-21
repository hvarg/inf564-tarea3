package dsnn

object dsnn {
  import org.apache.spark._
  import org.apache.spark.graphx.{Graph, Edge}

  /* Toma dos lista ordenadas de nodos y retorna una lista ordenada con los k valores
   * menores. De esta forma se calculan los k vecinos mas cercanos. */
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

  /* Calcula la vecindad compartida entre a y b, retorna el numero de elementos
   * que estan en ambos conjuntos. */
  def SNN (a: Array[(Long, Int)], b: Array[(Long, Int)]): Int = {
    var c = 0
    for (x <- a; y <- b) {
      if (x._1 == y._1) c += 1
    }
    c
  }

  def main (args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dsnn").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    // Parametros del algoritmo:
    var file   = "bitcoinalpha.csv"
    var k      = 10
    var eps    = 3
    var minpts = 3
    if (args.length > 0) file   = args(0)
    if (args.length > 1) k      = args(1).toInt
    if (args.length > 2) eps    = args(2).toInt
    if (args.length > 3) minpts = args(3).toInt
    println("| file: "+file+" | k = "+k+" | EPS = " + eps + " | MinPts = " + minpts +" |")

    //Se lee el archivo y se transforma en un conjunto de arcos
    val lines = sc.textFile(file).map(line => line.split(','))
    val data = lines.map( arr => {
      // Se transforma el rating a distancia.
      Edge[Int](arr(0).toLong, arr(1).toLong, 10 - arr(2).toInt)
    })

    // Los arcos se transforman en un grafo.
    val graph = Graph.fromEdges[Int, Int](data, 0).cache()

    // Se calculan los k vecinos más cercanos a cada nodo.
    val kclosest = graph.aggregateMessages[Array[(Long, Int)]](
      triplet => { triplet.sendToSrc( Array((triplet.dstId, triplet.attr)) ) },
      (a,b) => { kMin(k, a, b) }
    )

    // Se crea la matriz con la vecindad compartida
    var snns = kclosest.cartesian(kclosest).filter(z=>{z._1._1 != z._2._1}).map(x => {
      (x._1._1, SNN(x._1._2,x._2._2))
    })

    // Se obtienen los core-points
    var core = snns.filter(x=>{x._2>eps}).countByKey.filter(x=>{x._2>minpts}) //core points

    println("core points:")
    println(core.keys)

    sc.stop()
  }
}
