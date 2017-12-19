import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import java.io.{BufferedWriter, FileWriter, File}

object Yaosheng_Zhang {
  def main(args :Array[String]): Unit = {
    val caseNum = args{0}.toInt
    val RatingData = args{1}
    val UserData = args{2}
    val support = args{3}.toInt
    val conf = new SparkConf().setMaster("local").setAppName("Yaosheng_Zhang")
    val sc = new SparkContext(conf)
    val inputUserData = sc.textFile(UserData)
    val inputRatingData = sc.textFile(RatingData)
    if (caseNum == 1) {
      case1(inputUserData, inputRatingData, support, sc)
    } else if (caseNum == 2) {
      case2(inputUserData, inputRatingData, support, sc)
    }
  }
  def case1(user : RDD[String], rating : RDD[String], support : Int, sc : SparkContext)  = {
    val tmpUser = user.map(line => (line.split("::")(0).toInt, line.split("::")(1))).filter(elem => elem._2 == "M")
    val tmpRating = rating.map(line => (line.split("::")(0).toInt, line.split("::")(1)))
    val file = new File("./OutputFiles")
    file.mkdir()
    val out = new BufferedWriter(new FileWriter(new File("./OutputFiles/Yaosheng_Zhang_SON.case1" +  "_" + support + ".txt")))
    out.write("")
    out.flush()
    val mapPhaseOne = tmpUser.join(tmpRating).map(line => (line._2._2, line._1))
    val reduceOriginalPairs = mapPhaseOne.groupByKey().sortByKey()
    val getRes = reduceOriginalPairs.map(elem => (elem._1.toInt, elem._2.to[collection.mutable.Set]))
    val partition = getRes.map(elem => (elem._1, elem._2)).partitionBy(new org.apache.spark.HashPartitioner(3))
    var resultList = new ListBuffer[List[(Int, collection.mutable.Set[Int])]]
    var phaseOne = partition.mapPartitionsWithIndex{
      (partIdx, iter) => {
        var part_map = scala.collection.mutable.Map[String,List[(Int, collection.mutable.Set[Int])]]()
        while(iter.hasNext){
          var part_name = "part_" + partIdx
          var elem = iter.next()
          if(part_map.contains(part_name)) {
            var elems = part_map(part_name)
            elems ::= elem
            part_map(part_name) = elems
          } else {
            part_map(part_name) = List[(Int, collection.mutable.Set[Int])]{elem}
          }
        }
        part_map.iterator
      }
    }
    for (list <- resultList) {
      var bool = true
      var i = 0
      var res3 = collection.mutable.Set.empty[List[(Int, collection.mutable.Set[Int])]]
      while (bool) {
        var res2 = list.combinations(i).to[collection.mutable.ListBuffer]
        for (elem <- res2) {
          res3.add(elem)
        }
        if (res3.isEmpty) {
          bool = false
        }
        val PhaseTwoKey = sc.parallelize(res3.toSeq)
        val PhaseTwoSet = PhaseTwoKey.toLocalIterator
        i = i + 1
      }
    }
    val res = getRes.filter{case(x, y) => y.size > support}.sortByKey()
    val res_1 = res.toLocalIterator.toList
    var j = 0
    for (e <- res_1) {
      if (j == res_1.size - 1) {
        out.append("(" + e._1 +")")
      } else {
        out.append("(" + e._1 +"), ")
      }
      j = j + 1
    }
    out.write('\n')
    var res1 = res.toLocalIterator.to[collection.mutable.ListBuffer]
    var i = 2
    var bool = true
    while (bool) {
      var res2 = res1.combinations(i).to[collection.mutable.ListBuffer]
      var res3 = helper(res2, support)
      if (res3._2.isEmpty) {
        bool = false
      }
      val combinationKey = sc.parallelize(res3._2.toSeq)
      val combinationSet = combinationKey.map(elem => (elem, 1)).join(res).map(elem => (elem._1, elem._2._2)).sortByKey()
      val combinationList = combinationSet.toLocalIterator.to[collection.mutable.ListBuffer]
      res1 = combinationList
      j = 0
      for (e <- res3._1) {
        if (j == res3._1.size - 1) {
          out.write(e._1.toString.substring(10))
        } else {
          out.write(e._1.toString.substring(10) + ", ")
        }
        j = j + 1
      }
      if(bool) {
        out.write('\n')
      }
      i = i + 1
    }
    out.flush()
    out.close()
  }
  def case2(user : RDD[String], rating : RDD[String], support : Int, sc : SparkContext)  = {
    val tmpUser = user.map(line => (line.split("::")(0).toInt, line.split("::")(1))).filter(elem => elem._2 == "F")
    val tmpRating = rating.map(line => (line.split("::")(0).toInt, line.split("::")(1)))
    val file = new File("./OutputFiles")
    file.mkdir()
    val out = new BufferedWriter(new FileWriter(new File("./OutputFiles/Yaosheng_Zhang_SON.case2" +  "_" + support + ".txt")))
    out.write("")
    out.flush()
    val mapPhaseOne = tmpUser.join(tmpRating).map(line => (line._1.toInt, line._2._2.toInt))
    val reduceOriginalPairs = mapPhaseOne.groupByKey().sortByKey()
    val getRes = reduceOriginalPairs.map(elem => (elem._1.toInt, elem._2.to[collection.mutable.Set]))
    val partition = getRes.map(elem => (elem._1, elem._2)).partitionBy(new org.apache.spark.HashPartitioner(3))
    var resultList = new ListBuffer[List[(Int, collection.mutable.Set[Int])]]
    var phaseOne = partition.mapPartitionsWithIndex{
      (partIdx, iter) => {
        var part_map = scala.collection.mutable.Map[String,List[(Int, collection.mutable.Set[Int])]]()
        while(iter.hasNext){
          var part_name = "part_" + partIdx
          var elem = iter.next()
          if(part_map.contains(part_name)) {
            var elems = part_map(part_name)
            elems ::= elem
            part_map(part_name) = elems
          } else {
            part_map(part_name) = List[(Int, collection.mutable.Set[Int])]{elem}
          }
        }
        part_map.iterator
      }
    }
    for (list <- resultList) {
      var bool = true
      var i = 0
      var res3 = collection.mutable.Set.empty[List[(Int, collection.mutable.Set[Int])]]
      while (bool) {
        var res2 = list.combinations(i).to[collection.mutable.ListBuffer]
        for (elem <- res2) {
            res3.add(elem)
        }
        if (res3.isEmpty) {
          bool = false
        }
        val PhaseTwoKey = sc.parallelize(res3.toSeq)
        val PhaseTwoSet = PhaseTwoKey.toLocalIterator
        i = i + 1
      }
    }
    val res = getRes.filter{case(x, y) => y.size > support}.sortByKey()
    val res_1 = res.toLocalIterator.toList
    var j = 0
    for (e <- res_1) {
      if (j == res_1.size - 1) {
        out.append("(" + e._1 +")")
      } else {
        out.append("(" + e._1 +"), ")
      }
      j = j + 1
    }
    out.write('\n')
    var res1 = res.toLocalIterator.to[collection.mutable.ListBuffer]
    var i = 2
    var bool = true
    while (bool) {
      var res2 = res1.combinations(i).to[collection.mutable.ListBuffer]
      var res3 = helper(res2, support)
      if (res3._2.isEmpty) {
        bool = false
      }
      val combinationKey = sc.parallelize(res3._2.toSeq)
      val combinationSet = combinationKey.map(elem => (elem, 1)).join(res).map(elem => (elem._1, elem._2._2)).sortByKey()
      val combinationList = combinationSet.toLocalIterator.to[collection.mutable.ListBuffer]
      res1 = combinationList
      j = 0
      for (e <- res3._1) {
        if (j == res3._1.size - 1) {
          out.write(e._1.toString.substring(10))
        } else {
          out.write(e._1.toString.substring(10) + ", ")
        }
        j = j + 1
      }
      if (bool) {
        out.write('\n')
      }
      i = i + 1
    }
    out.flush()
    out.close()
  }
  def helper(rdd : ListBuffer[ListBuffer[(Int, collection.mutable.Set[Int])]], sup : Int) : (ListBuffer[(ListBuffer[Int], Int)], collection.mutable.Set[Int]) ={
    val listOut : ListBuffer[(ListBuffer[Int], Int)] = scala.collection.mutable.ListBuffer.empty[(ListBuffer[Int], Int)]
    val setOut : collection.mutable.Set[Int] = scala.collection.mutable.Set.empty[Int]
    for (e <- rdd) {
      val e_size = e.size
      var i = 0
      val list : ListBuffer[Int] = scala.collection.mutable.ListBuffer.empty[Int]
      var set_tmp = e{0}._2
      while (i < e_size) {
        list.append(e{i}._1)
        set_tmp = set_tmp & e{i}._2
        i = i + 1
      }
      val size = set_tmp.size
      if (size >= sup) {
        listOut.append((list, size))
        for (el <- list) {
          setOut.add(el)
        }
      }
    }
    return (listOut, setOut)
  }
}
