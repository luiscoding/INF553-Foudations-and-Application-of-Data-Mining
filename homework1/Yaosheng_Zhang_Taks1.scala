import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Yaosheng_Zhang_Taks1 {
    def main(args : Array[String]) : Unit = {
        val conf = new SparkConf().setAppName("Yaosheng_Zhang_Taks1").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val userInfo = sc.textFile(args{0}).map(line => (line.split("::")(0), line.split("::")(1)))
        val ratingInfo = sc.textFile(args{1}).map(line => (line.split("::")(0),  (line.split("::")(1),  line.split("::")(2))))
        val res = ratingInfo.join(userInfo).values.map(elem => ((elem._1._1, elem._2), elem._1._2))
        val mapCount = sc.parallelize(res.countByKey().toSeq)
        val mapSum = res.reduceByKey((x, y) => (Integer.parseInt(x) + Integer.parseInt(y)).toString)
        val result = mapSum.join(mapCount).map(elem => elem._1._1 + "," + elem._1._2 + "," +(elem._2._1.toDouble / elem._2._2.toDouble).toString).sortBy(elem => (elem.split(",")(0).toInt, elem.split(",")(1)))
        result.repartition(1).saveAsTextFile("Taks1_" + System.currentTimeMillis())
    }
}
