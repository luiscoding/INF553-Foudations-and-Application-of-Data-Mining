import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.{File, PrintWriter}
object Yaosheng_Zhang_Task2 {
    def main(args: Array[String]) : Unit = {
        val conf = new SparkConf().setAppName("Yaosheng_Zhang_task2").setMaster("local[1]")
        val sc = new SparkContext(conf)
		val start_time = System.nanoTime()
        val rating_file = args(0)
        val testing_file = args(1)
        val output_file = new PrintWriter(new File("Yaosheng_Zhang_result_task2.txt"))
        val rate_text = sc.textFile(rating_file)
        val test_text = sc.textFile(testing_file)
        val header_rate = rate_text.first()
        val header_test = test_text.first()
        val rate_rdd = rate_text.filter(elem => elem != header_rate).map(elem => ((elem.split(",")(0).toInt, elem.split(",")(1).toInt),
            elem.split(",")(2).toDouble))
        val test_rdd = test_text.filter(elem => elem != header_test).map(elem => (elem.split(",")(0), elem.split(",")(1))).map{
            case (userid, moveid) => ((userid.toInt, moveid.toInt),1)}
        val test_array = test_rdd.collect().toMap
        val train = rate_rdd.filter{rdd => !test_array.keySet.contains(rdd._1)}
        val user_length =  train.map{case((user, product),rate_text) => (user, rate_text)}.groupByKey().map(elem => (elem._1, elem._2.size))
        val user_sum =  train.map{case((user, product),rate_text) => (user, rate_text)}.reduceByKey(_ + _)
        val user_avg = user_sum.join(user_length).map{case(user, (sum, len)) => (user, sum / len)}
        val train_res = train.map{case((user,product),rate_text) => (product, (user,rate_text))}
        val train_res_ = train_res.join(train_res)
        val matrix = train_res_.map{case(product,((useri, ratei),(userj, ratej))) => ((useri, userj),product,(ratei, ratej))}
        val matrix_group = matrix.map{case((useri, userj),product,(ratei, ratej)) => ((useri, userj), (ratei, ratej))}.groupByKey()
        val length = matrix_group.map(elem => (elem._1, elem._2.size))
        val group_i = matrix.map{case((useri, userj),product,(ratei, ratej)) => ((useri, userj),ratei)}.reduceByKey(_ + _)
        val group_j = matrix.map{case((useri, userj),product,(ratei, ratej)) => ((useri, userj),ratej)}.reduceByKey(_ + _)
        val avg_group = group_i.join(group_j).join(length).map{case((useri, userj),((ratei,ratej),len)) => ((useri, userj),(ratei / len, ratej/len))}
        val map_group = matrix.filter(elem => elem._1._1 != elem._1._2).map{case((useri, userj),product,(ratei, ratej)) => ((useri, userj),(ratei, ratej))}
        val mean = map_group.join(avg_group).map{case((useri, userj),((ratei, ratej),(meani, meanj))) => ((useri, userj),((ratei - meani),(ratej - meanj)))}
        val nmi = mean.map{case((useri, userj),(meani,meanj)) => ((useri, userj),(meani * meanj))}reduceByKey(_ + _)
        val demni = mean.map{case((useri, userj),(meani,meanj)) => ((useri, userj), meani * meani, meanj * meanj)}
        val demnii = demni.map{case((useri, userj), meanii,  meanjj) => ((useri, userj),meanii)}.reduceByKey(_ + _)
        val demniij = demni.map{case((useri, userj), meanii, meanjj) => ((useri, userj),meanjj)}.reduceByKey(_ + _)
        val demn_com = demnii.join(demniij).map{case((useri, userj),(meanii, meanjj)) => ((useri, userj), Math.sqrt(meanii) * Math.sqrt(meanjj))}
        val weight = nmi.join(demn_com).map{case((useri, userj), (nom, denom)) =>
            if (denom == 0)
                ((useri, userj), 0.0)
            else ((useri, userj), (nom / denom))
        }
        val textdata = test_rdd.map{case((user, product), num) => (product,(user))}
        val texttrain = textdata.join(train_res).map{case(product, (useri,(userj, ratej))) => ((useri, userj),(product, ratej))}
        val textmean = texttrain.join(avg_group).map{case((useri, userj),((product, ratej),(meani, meanj))) => ((useri, userj),(product, ratej - meanj))}
        val textratewight = textmean.join(weight)
        val predictnom = textratewight.map{case((useri, userj),((product, rmean), w)) => ((useri,product), rmean * w)}.reduceByKey(_ + _)
        val predictdenom = textratewight.map{case((useri, userj),((product, rmean), w)) => ((useri,product), Math.abs(w))}.reduceByKey(_ + _)
        val predicright = predictnom.join(predictdenom).map{case((user, product), (n, d)) =>
            if (d == 0) (user, (product, 0.01))
            else (user, (product, n / d))
        }
        val predict = user_avg.join(predicright).map{case(user,((left), (product, right))) => ((user, product), left + right)}
        val maprightpart = predict.collect().toMap
        val rest = test_rdd.filter(elem => !maprightpart.keySet.contains(elem._1))
        val restrate = rest.map{case((user, product), num) => (user, (product))}.join(user_avg).map{case(user,(product, avg)) => ((user,product), avg)}
        val predicttotal = predict.++(restrate)
        val preductNomal = predicttotal.map{case((user, product), pred) =>
            if (pred <= 0) ((user, product), 0.3)
            else if (pred >= 5) ((user, product), 5.0)
            else ((user, product), pred)
        }
        val diff = rate_rdd.join(preductNomal).map{case((user, product),(t, predic)) => ((user, product), Math.abs(t - predic))}
        val count0 = diff.filter(elem => elem._2 >= 0 && elem._2 < 1).count()
        val count1 = diff.filter(elem => elem._2 >= 1 && elem._2 < 2).count()
        val count2 = diff.filter(elem => elem._2 >= 2 && elem._2 < 3).count()
        val count3 = diff.filter(elem => elem._2 >= 3 && elem._2 < 4).count()
        val count4 = diff.filter(elem => elem._2 >= 4).count()
        val RMSE = rate_rdd.join(preductNomal).map{case((user, product),(t, predic)) =>
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
        print("The total execution time taken is " +  ((end_time - start_time) / 1000000000) + " sec.")
    }
}
