import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.Ordering
import java.nio.file.{StandardOpenOption, Paths, Files}
import java.util.Scanner
import org.jsoup.Jsoup
import scala.io.Source
import org.apache.spark.graphx._


object functionToCallPageRank {

  val iters = 5
  val topCount = 100
  var Datafile = "C:\\dataset\\freebase-wex-2009-01-12-articles.tsv"
  var URL = "local"
  
  

  val sparkres = "sparkImplementation-Result.log"
  val graphxres = "GraphXimplementation-Result.log"
  val top100universities = "top100-Universities-List.log"

  def main(args: Array[String]) {
    
    
      //Extract uRL of masternode
    if (args.length >= 2) {
      URL = args(1).toString.trim
    }

   //Extract location of the datafile
    if (args.length >= 1) {
      Datafile = args(0).toString.trim
    }
    println("Location of datafile : " + Datafile)

  
    println("Master Node to implement code : " + URL)

    println("Top count of articles : " + topCount)

    val Config = new SparkConf().setAppName("PageRank-43394921")
    val sc = new SparkContext(Config)
    var t = true
    var s = new Scanner(System.in)

    var List1: Graph[(Double, String), Double] = null

    while (t) {
      Thread.sleep(500)
      println("===============================================================================")
      println("Enter option to run: \n\t1 - Pagerank on GraphX(Default) \n\t2 - Pagerank on Spark \n\t3 - Top 100 List")
      println("Waiting..")
      var l = s.nextLine()
      var opt = 1
      if (l.length > 0) {
        opt = l.toInt
      }
      println("Read " + opt)

      if (opt == 1) {
        List1 = PagerankGraphX.printTopPages(Datafile, URL, topCount, iters, graphxres, sc)
      }

      if (opt == 2) {
        SparkPageRank.PrintTopArticles(Datafile, URL, topCount, iters, sparkres, sc)
      }

      if (opt == 3) {
        if (List1 == null) {
          println("Starting evaluation of Pagerank using graphX")
          List1 = graphXpageRank.printTopArticles(DataFile, URL, topCount, iters, graphxres, sc)
        }
        Files.deleteIfExists(Paths.get(top100result))

        write("\nUniversity List ==============================================================", top100result)
        val l = Source.fromURL(getClass.getResource("/University List.txt")).getLines().toList

        var p = List1.vertices.top(List.triplets.count().toInt) {
          Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
        }.filter{ x => 
          l contains (x._2._2)
        }.take(100).foreach(x => write("\n" + x._2._2 + " university has rank : " + x._2._1, top100result))
      }

      if (opt == -1) {
        System.exit(0)
        sc.stop()
      }
    }
  }

  def write(r: String, path: String) = {
    println(r)
    Files.write(Paths.get(path), r.getBytes("utf-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }

  def scrapeList(): List[String] = {
    var universities = List[String]()
    var URLS = for (i <- 2 to 27) yield s"http://www.4icu.org/reviews/index$i.htm"
    for (url <- URLS) {
      Thread.sleep(100)
      println("Listing " + url)
      var file = Jsoup.connect(url).maxBodySize(204800000).timeout(100000).get()
     //Extract all links
      var links = file.select("img[src$=.png][width=16]");
      var it = links.iterator()
      while (it.hasNext) {
        var link = it.next()
        var u = link.attr("alt")
        universities = universities ::: (List(u.trim))
      }
    }
    universities
  }
}