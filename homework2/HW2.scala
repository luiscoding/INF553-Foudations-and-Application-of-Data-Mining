import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.util.control.Breaks._
import collection.mutable.{HashMap, ListBuffer}
import scala.collection.mutable
import java.io._


object SON_final {
  def main(args: Array[String]): Unit = {

    val start_time = System.currentTimeMillis()

    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().appName("Frequent Itemsets").master("local[*]").getOrCreate()

    val ratingsSchema = StructType(Seq(
      StructField("userID", IntegerType),
      StructField("movieID", IntegerType)
    ))
    val usersSchema = StructType(Seq(
      StructField("userID", IntegerType),
      StructField("Gender", StringType)
    ))



    val caseX = 2
    val ratingsFile = ratings.dat
    val usersFile = users.dat
    val support_threshold = 3000

    val output_filename = "/usr/local/spark-2.1/result.txt"
    val out_file = new File(output_filename)
    val bw = new BufferedWriter(new FileWriter(out_file))


    val ratingsCSV = sparkSession.read.schema(ratingsSchema).csv(ratingsFile)
    val usersCSV = sparkSession.read.schema(usersSchema).csv(usersFile)
    
    var basketsRDD = ratingsCSV.rdd.map(x => (x.getAs[Int]("userID"), Seq(x.getAs[Int]("movieID")))).reduceByKey(_ ++ _).map(x => x._2) // (userId, Seq(movieId1, movieId2, ...))
    if (caseX == 2){
      basketsRDD = ratingsCSV.rdd.map(x => (x.getAs[Int]("movieID"), Seq(x.getAs[Int]("userID")))).reduceByKey(_ ++ _).map(x => x._2) // (movieId, Seq(userId1, userId2, ...))
    }

    val basketsRDD0 = basketsRDD.map(x=> (x.distinct, 0)).cache() // (Set.Seq(Int), Int)


    //val users = basketsRDD.count()
    //println("Users: " + users)
    //println(basketsRDD.map(x => x.size).reduce(_ + _).toFloat / users)
    //println(basketsRDD.map(x => x.size).foreach(println))


    val totalBasketsBC = sparkSession.sparkContext.broadcast(basketsRDD0.count())
    val supportBC = sparkSession.sparkContext.broadcast(support_threshold)

    // each record in basketsRDD is a list of IDs, we need to find the maximum size of these lists
    val max_combinations = basketsRDD0.max()(new Ordering[(Seq[Int], Int)]() {
      override def compare(x: (Seq[Int], Int), y: (Seq[Int], Int)): Int =
        Ordering[Int].compare(x._1.size, y._1.size)
    })._1.size

    var frequentItemSets = Seq[Seq[Int]]() // it will be initialized non-Empty after the first iteration
    var frequentSingletons = Set[Int]() // they will be updated on every iteration, based on monotonicity
    var basketsRDDX = basketsRDD0


    //basketsRDD.flatMap( x => x).map( x => (x,1)).reduceByKey(_ + _).collect().sortBy(x => x._2).foreach(println)
    //System.exit(1)

    breakable({ // The below loop breaks if a for a combination there are no frequent itemsets. In this case no higher combinations could exist
        for (combinationValue <- 1 to max_combinations) { // Iterate through all the possible number of combinations that could exist

          val frequentSinglesBC = sparkSession.sparkContext.broadcast(frequentSingletons.toSeq)

          val candidatesRDD = basketsRDDX.mapPartitions(baskets => { // The candidates found across all the partitions

            val frequentSingletons = frequentSinglesBC.value.toSet
            //val frequentSingletons = frequentCombinations.flatten.sorted.toSet
            val combinationsMap = new mutable.HashMap[Seq[Int], Int]() // A HashMap counter within each partition
            var chunkSize = 0F // number of baskets in this chunk


            baskets.foreach(basket => { // Loop through the baskets of this partition

              val basketItems = basket._1.toSet
              chunkSize += 1F
              if (combinationValue != 1){
                //val basketCombinations = basketItems.toList.combinations(combinationValue)
                val intersectionCombs = basketItems.intersect(frequentSingletons)
                if (intersectionCombs.size >= combinationValue){
                  val combinations = intersectionCombs.toList.combinations(combinationValue)
                  combinations.foreach({ combination =>
                    val sortedComb = combination.sorted
                    updateOrAdd(combinationsMap, sortedComb, sortedComb -> 1, (v: Int) => v + 1)
                  })
                }
              }else{ // Consider all items as 1
                basketItems.foreach({ item =>
                  updateOrAdd(combinationsMap, Seq(item), Seq(item) -> 1, (v: Int) => v + 1)
                })
              }

            }) // end of Partition Baskets Loop

            val chunkPercentage = chunkSize / totalBasketsBC.value // size percentage of this chunk out of total
            val chunkThreshold = 0.9 * chunkPercentage * supportBC.value // threshold for this chunk based on size
            combinationsMap.keySet.foreach(key => if (combinationsMap.get(key).get < chunkThreshold) combinationsMap.remove(key)) // Remove from the map itemsets not frequent in the chunk

            val chunkFrequentItemsets = combinationsMap.keySet.map(x=> (x, combinationsMap.get(x).get))
            chunkFrequentItemsets.toIterator

          }).reduceByKey(_ + _) // Add up all the frequencies of the Candidate Frequent Itemsets found across the partitions

          val frequentItemSetRDD = candidatesRDD.filter(x => x._2 >= support_threshold)
          frequentItemSets = frequentItemSetRDD.map(x => x._1).collect()

          if (frequentItemSets.nonEmpty) {

            val sb = new StringBuilder
            /** ORDER FREQUENT ITEMSETS IN ASCENDING ORDER **/
            // //frequentItemSets.sortWith({ case (x: Seq[Int], y:Seq[Int]) => // NO NEED FOR EXTRA SORT
            frequentItemSets.map(x => x.sorted).sortWith({ case (x: Seq[Int], y:Seq[Int]) =>
              if (x.head < y.head) true
              else if (x.head > y.head) false
              else {
                var pointer = 0
                while (pointer < x.length && x.apply(pointer) == y.apply(pointer)){
                  pointer += 1
                }
                if (pointer >= x.length) true
                else if (x.apply(pointer) < y.apply(pointer)) true
                else false
              }
            }).foreach(itemset => {
              var comma1 = ""
              sb.append("(")
              itemset.foreach(item => {
                sb.append(comma1)
                sb.append(item)
                comma1 = ", "
              })
              sb.append("), ")
            })
            sb.delete(sb.length-2, sb.length)
            println(sb)
            bw.write(sb.toString() + "\n")

            frequentSingletons = frequentItemSets.flatten.toSet // If frequent itemsets found for this combination forward the singletons to the next iteration
            //println("Total Singletons: " + frequentSingletons.size)
            println()
            bw.write("\n")

            val frequentSingletonsBC = sparkSession.sparkContext.broadcast(frequentSingletons)
            basketsRDDX = basketsRDDX.mapPartitions(baskets => {
              val freqSingles = frequentSingletonsBC.value
              val newBaskets = baskets.map(x => {
                (x._1.toSet.intersect(freqSingles).toSeq, x._2)
              }).filter(_._1.nonEmpty)
              newBaskets
            })

            //println(basketsRDDX.count())

          } else {
            //println("FINISH")
            break // if no frequent items found then they cannot exist for higher combination values
          }

        }

    }) // end of breakable

    val end_time = System.currentTimeMillis()
    //println("TOTAL EXECUTION TIME in secs: " + (end_time - start_time)/1000 + " secs")
    bw.close()

  }

  def updateOrAdd[K, V](m: collection.mutable.Map[K, V], k: K, kv: (K, V),
                        f: V => V) {
    m.get(k) match {
      case Some(e) => m.update(k, f(e))
      case None    => m += kv
    }
  }


}
