import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.{File, PrintWriter}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object Yaosheng_Zhang_Task1 {
    def main(args: Array[String]) : Unit = {
        val start_time = System.nanoTime()
        val rating_file = args(0)
        val inputFileTest = args(1)
        val output_file = new PrintWriter(new File("Yaosheng_Zhang_result_task1.txt"))
        val conf = new SparkConf().setAppName("Yaosheng_Zhang_Task1").setMaster("local[1]")
        val sc = new SparkContext(conf)
        val rate_text = sc.textFile(rating_file)
        val text = sc.textFile(inputFileTest)
        val header_rate = rate_text.first()
        val header_test = text.first()
        val rateForm = rate_text.filter(elem => elem != header_rate).map(elem => (elem.split(",")(0).toInt, elem.split(",")(1).toInt,
            elem.split(",")(2).toDouble) match {case (user, product, rate_text) => Rating(user.toInt, product.toInt, rate_text.toDouble)})
        val rate_rdd = rate_text.filter(elem => elem != header_rate).map(elem => ((elem.split(",")(0).toInt, elem.split(",")(1).toInt),
            elem.split(",")(2).toDouble))
        val test_rdd = text.filter(elem => elem != header_test).map(elem => (elem.split(",")(0), elem.split(",")(1))).map{
            case (userid, moveid) => ((userid.toInt, moveid.toInt),1)}
        val texting = text.filter(elem => elem != header_test).map(elem => (elem.split(",")(0).toInt, elem.split(",")(1).toInt))
        val test_array = test_rdd.collect().toMap
        val train = rate_rdd.filter{rdd => !test_array.keySet.contains(rdd._1)}
        val user_length =  train.map{case((user, product),rate_text) => (user, rate_text)}.groupByKey().map(elem => (elem._1, elem._2.size))
        val user_sum =  train.map{case((user, product),rate_text) => (user, rate_text)}.reduceByKey(_ + _)
        val user_avg = user_sum.join(user_length).map{case(user, (sum, len)) => (user, sum / len)}
        val training = train.map(elem => (elem._1._1, elem._1._2, elem._2) match {case (user, product, rate_text) =>
            Rating(user.toInt, product.toInt,rate_text.toDouble)})
        val rank = 10
        val numIterations = 20
        val model = ALS.train(training, rank, numIterations, 0.01)
        val predictions = model.predict(texting).map{case Rating(user, product, rate_text) => ((user, product),rate_text)}
        val maprightpart = predictions.collect().toMap
        val rest = test_rdd.filter(elem => !maprightpart.keySet.contains(elem._1))
        val restrate = rest.map{case((user, product), num) => (user, (product))}.join(user_avg).map{case(user,(product, avg)) => ((user,product), avg)}
        val predicttotal = predictions.++(restrate)
        val preductNomal = predicttotal.map{case((user, product), pred) =>
            if (pred <= 0) ((user, product), 0.0)
            else if (pred >= 5) ((user, product), 5.0)
            else ((user, product), pred)
        }
        val ratesAndPreds = rateForm.map { case Rating(user, product, rate_text) => ((user, product), rate_text)}.join(preductNomal)
        val count0 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 0 && elem._2 < 1).count()
        val count1 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 1 && elem._2 < 2).count()
        val count2 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 2 && elem._2 < 3).count()
        val count3 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 3 && elem._2 < 4).count()
        val count4 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 4).count()
        val RMSE = ratesAndPreds.map{case((user, product),(t, predic)) =>
            val err = t - predic
            err * err
        }.mean()
        output_file.print("UserId,MovieId,Pred_rating" + "\n")
        val iter = preductNomal.sortByKey().map(elem => elem._1._1 + "," + elem._1._2 + "," + elem._2).toLocalIterator
        while(iter.hasNext) {
            output_file.print(iter.next() + "\n")
        }
        output_file.close()
        print(">= 0 and < 1: " + count0 + "\n")
        print(">= 1 and < 2: " + count1 + "\n")
        print(">= 2 and < 3: " + count2 + "\n")
        print(">= 3 and < 4: " + count3 + "\n")
        print(">= 4: " + count4 + "\n")
        print("RMSE = " + Math.sqrt(RMSE) + "\n")
        val end_time = System.nanoTime()
        val time = (end_time - start_time) / 1000000000
        print("The total execution time taken is " +  time  + " sec.")
    }
}
