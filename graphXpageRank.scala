import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.math.Ordering
import java.nio.file.{FileAlreadyExistsException, StandardOpenOption, Files, Paths}
import java.nio.file.attribute.FileAttribute


object graphXpageRank {
  
  var Time = System.currentTimeMillis()
  def printTopArticles(Datafile: String, URL: String, topC: Int, Iterations: Int, path: String, sc: SparkContext): Graph[(Double, String), Double] = {
    Files.deleteIfExists(Paths.get(path))
    
    def pageHash(Title: String): VertexId = {
    Title.toLowerCase.replace(" ", "").hashCode.toLong
    }
    
    //Read data file
    val wikifile: RDD[String] = sc.textFile(Datafile).coalesce(20)
    case class wikiArticle(ID: Long, title: String, xml: String)
    var r: String = ""
    //map and split the data
    val wikiarticles = wikifile.map(_.split('\t')).
     filter(line => line.length > 1).
      map(line => new wikiArticle(line(0).trim.toLong, line(1).trim, line(3).trim)).cache()

    writetofile("Total numebr of articles is " + wikiarticles.count(), path)

    val vertices: RDD[(VertexId, String)] = wikiarticles.map(a => (pageHash(a.title), a.title))

    writetofile("Total n " + vertices.count(), path)

    val pattern = "<target>.+?<\\/target>".r

    val edges: RDD[Edge[Double]] = articles.flatMap { a =>
      val srcId = pageHash(a.title)
      pattern.findAllIn(a.xml).map { link =>
        val dstId = pageHash(link.replace("<target>", "").replace("</target>", ""))
        Edge(srcId, dstId, 1.0)
      }
    }
    writetofile("Total number of edges - " + edges.count(), path)

    val graph = Graph(vertices, edges, "").subgraph(vpred = { (v, d) => d.nonEmpty }).cache

    writetofile("Final graph built with vertices - edges  " + graph.vertices.count() + "\t Edges " + graph.edges.count() + " Triplets " + graph.triplets.count, path)
    writetofile("Time taken to build graph : " + (System.currentTimeMillis() - Time), path)

    Time = System.currentTimeMillis()

    val prGraph: Graph[Double, Double] = graph.staticPageRank(iters).cache

    val title2: Graph[(Double, String), Double] = graph.outerJoinVertices(prGraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }

    var page = title2.vertices.top(titleAndPrGraph.triplets.count().toInt) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }.take(topC).foreach(t => {
      write(t._2._2 + " page has rank: " + t._2._1, path)
    })
    writetofile("Total time taken =using graphX is " + (System.currentTimeMillis() - Time ), path)
    title2
  }
//define write function to write to log file
  def writetofile(r: String, path: String) = {
    print(r + "\n")
    Files.write(Paths.get(path), (r + "\n").getBytes("utf-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }
}