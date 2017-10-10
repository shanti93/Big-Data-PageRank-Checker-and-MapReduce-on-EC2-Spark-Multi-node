import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.nio.file.{StandardOpenOption, Paths, Files}

object SparkPageRank {

  def PrintTopArticles(WikiData: String, MasterUrl: String, TopCount: Int, Iterations: Int, Path: String, sc: SparkContext) {

    //Read the data file
    val Wiki: RDD[String] = sc.textFile(WikiData, 1)
    var Time = System.currentTimeMillis()
     Files.deleteIfExists(Paths.get(Path))
    case class wikiArticle(val ID: Long, val Title: String, val xml: String)

    val totalArticles = Wiki.map(_.split('\t')).
      filter(line => line.length > 1).
      map(l => new wikiArticle(l(0).trim.toLong, l(1).trim, l(3).trim)).cache()

    Writetofile("Total count of Articles is " + totalArticles.count(), Path)

    val Pattern = "<target>.+?<\\/target>".r

    val Links: RDD[(String, Iterable[String])] = totalArticles.flatMap { art =>

      Pattern.findAllIn(art.xml).map { Link =>
        val Id2 = Link.replace("<target>", "").replace("</target>", "")
        (art.Title, Id2)
      }
    }.distinct().groupByKey().cache()

    Writetofile("Total number of Iterations for links is " + Links.count(), Path)

    var Ranks = Links.mapValues(v => 1.0)

    //Implement pagerank for number of iterations
    for (i <- 1 to Iterations) {

      println("Iteration ====================================> " + i)
      val contribs = Links.join(Ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      println("Contribs ====================================> " + contribs.count())
      Ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      //Main logic of pagerank
      println("Total number of Ranks is " + Ranks.count())
    }


    var outputBySpark = Ranks.sortBy(_._2, false).collect()

    //Write top articles based on required count to log file
    outputBySpark.take(TopCount).foreach(tup => {
      Writetofile(tup._1 + " has rank - " + tup._2, Path)
    })
    //Output time taken to compare with GraphX implementation
    Writetofile("Total time taken to compute using naive Spark implementation " + (System.currentTimeMillis() - Time ), Path)
  }

  def Writetofile(r: String, path: String) = {
    print(r + "\n")
    Files.write(Paths.get(path), (r + "\n").getBytes("utf-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }
}