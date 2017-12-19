import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Yaosheng_Zhang_Taks2 {
    def main(args : Array[String]) : Unit = {
        val conf = new SparkConf().setAppName("hw1").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val userData = sc.textFile(args{0}).map(line => (line.split("::")(0), line.split("::")(1))) // (UserId, Gender)
        val ratingData = sc.textFile(args{1}).map(line => (line.split("::")(0), (line.split("::")(1), line.split("::")(2)))) // (UserId, (MovieId, rating))
        val movieData = sc.textFile(args{2}).map(line => (line.split("::")(0), line.split("::")(2))) // (MovidId, Genrea)
        val joinUserAndRating = userData.join(ratingData).values.map(elem => (elem._2._1, (elem._1, elem._2._2)))
        val joinGenre = joinUserAndRating.join(movieData).values.map(elem => ((elem._2, elem._1._1), elem._1._2))
        val countMap = sc.parallelize(joinGenre.countByKey().toSeq)
        val reduceGenre = joinGenre.reduceByKey((x, y) => (x.toInt + y.toInt).toString)
        val res = countMap.join(reduceGenre).map(elem => elem._1._1 + "," + elem._1._2 + "," + elem._2._2.toDouble / elem._2._1.toDouble).sortBy(elem => (elem.split(",")(0), elem.split(",")(1)))
        res.repartition(1).saveAsTextFile("Taks2_" + System.currentTimeMillis())
    }
}
