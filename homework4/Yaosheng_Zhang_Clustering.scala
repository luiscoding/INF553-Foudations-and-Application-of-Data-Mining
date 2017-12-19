import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, PriorityQueue}
import java.io._

object Yaosheng_Zhang_Clustering {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd_text = sc.textFile(args{0}).filter(elem => !elem.equals(""))
    val clustering_number = args{1}.toInt

    val list = rdd_text.map(MapHelper).toLocalIterator.to[collection.mutable.ListBuffer]
    var current_cluster = CurrentCluster(list)
    var pairwise_distance = PairWiseDistance(current_cluster)
    var priority_queue = EnterPQ(pairwise_distance)

    while (current_cluster.size > clustering_number) {
      pairwise_distance = PairWiseDistance(current_cluster)
      priority_queue = EnterPQ(pairwise_distance)
      val pairwise = priority_queue.dequeue()
      val list_index_tmp =  ListBuffer.empty[Int]
      list_index_tmp.append(pairwise._1._1, pairwise._1._2)
      val list_index = list_index_tmp.distinct.sorted
      current_cluster = MergeLists(current_cluster, list_index)
    }
    val count = current_cluster.map(Count)
    WriteFile(current_cluster, count)
  }


  /*  Function MapHelper：
  *   Input :- each line from text
  *   transfer the first 4 number to Double
  *   Output :- (list contains 4 Doubles, string)
  *
  *   example of input : 5.1,3.5,1.4,0.2,Iris-setosa
  *   example of output: (List(5.1, 3.5, 1.4, 0.2),Iris-setosa)
  * */
  def MapHelper(str : String) : (List[Double], String) = {
    val list = new ListBuffer[Double]
    val str_split = str.split(",")
    var i = 0
    while (i < str_split.size - 1) {
      list += str_split{i}.toDouble
      i += 1
    }
    (list.toList, str_split{str_split.size - 1})
  }


  /*  Function EuclideanDistance
  *   Input :- o1 : (List[Double], String), o2 : (List[Double], String)
  *   zip tow lists in o1 and o2 then calculate euclidean distance
  *   Output :- return euclidean distance
  * */
  def EuclideanDistance(o1 : (List[Double], String), o2 : (List[Double], String)) : Double = {
    val zip_lists = o1._1.zip(o2._1)
    val subtract_lists = zip_lists.map(elem => math.pow(elem._1 - elem._2, 2.0))
    val distance = math.sqrt(subtract_lists.sum)
    distance
  }


  /*  Function PairWiseDistance
  *   Calculate all distance between any pairs(first peel of the listBuffer)
  *   Input :- current_cluster : ListBuffer[ListBuffer[(List[Double], String)]]
  *   Output :- distance between all paris --> List[((Int, Int), Double)]
  * */
  def PairWiseDistance(current_cluster : ListBuffer[ListBuffer[(List[Double], String)]]) : ListBuffer[((Int, Int), Double)] = {
    val list = collection.mutable.ListBuffer.empty[(List[Double], String)]
    var i = 0
    while (i < current_cluster.size) {
      val sub_list = current_cluster.apply(i)
      var point = sub_list.head
      if (sub_list.size > 1) {
        point = Centroid(sub_list)
      }
      list.append(point)
      i += 1
    }

    i = 0
    val distance_listBuffer = collection.mutable.ListBuffer.empty[((Int, Int), Double)]
    while (i < list.size - 1) {
      var j = i + 1
      while (j < list.size) {
        val distance = EuclideanDistance(list{i}, list{j})
        val index = (i, j)
        val list_element = (index, distance)
        distance_listBuffer.append(list_element)
        j = j + 1
      }
      i += 1
    }
    distance_listBuffer
  }


  /*  Function EnterPQ
  *   Add pairwise distance to Priority Queue
  *   return Priority Queue in an ascending order
  * */
  def EnterPQ(list : ListBuffer[((Int, Int), Double)]) : mutable.PriorityQueue[((Int, Int), Double)] = {
    implicit val ord : Ordering[((Int, Int), Double)] = Ordering.by(-_._2)
    val priority_queue = new mutable.PriorityQueue[((Int, Int), Double)]()
    priority_queue.++(list)
  }


  /*  Function Centroid
  *   calculate centroid among points
  *   Input :- input_list : ListBuffer[(List[Double], String)]
  *   Output :- element_return : (List[Double], String)
  *   return centroid
  * */
  def Centroid(input_list : ListBuffer[(List[Double], String)]) : (List[Double], String)= {
    val list = input_list.map(elem => elem._1)
    var i = 0
    val size = list.size
    val centroid_coordinate = ListBuffer(0.0, 0.0, 0.0, 0.0)
    while (i < list.size) {
      var j = 0
      while (j < list{i}.size) {
        centroid_coordinate{j} += list{i}{j}
        j += 1
      }
      i += 1
    }
    val coordinate_centroid = centroid_coordinate.map(elem => elem / size).toList
    val element_return = (coordinate_centroid, "centroid")
    element_return
  }


  /*  Function CurrentCluster
  *   Input :- input : ListBuffer[(List[Double], String)]
  *   Put every element of type (List[Double], String) into a ListBuffer which satisfied the algorithm's assumption
  *   that every single point represents a cluster itself
  *   OutPut :- output_buffer : ListBuffer[ListBuffer[(List[Double], String)]]
  * */
  def CurrentCluster(input : ListBuffer[(List[Double], String)]) : ListBuffer[ListBuffer[(List[Double], String)]]= {
    val output_buffer = ListBuffer.empty[ListBuffer[(List[Double], String)]]
    for (i <- input) {
      val listBuffer = ListBuffer.empty[(List[Double], String)]
      listBuffer.append(i)
      output_buffer.append(listBuffer)
    }
    output_buffer
  }


  /*  Function MergeLists
   *  Input :- current_cluster : ListBuffer[ListBuffer[(List[Double], String)]] AND listBuffer_int : ListBuffer[Int]
   *  According to indices given by listBuffer_int, merge lists which are represented by indices in listBuffer_int
   *  Output :- current_cluster : ListBuffer[ListBuffer[(List[Double], String)]]
   **/
  def MergeLists(current_cluster : ListBuffer[ListBuffer[(List[Double], String)]], listBuffer_int : ListBuffer[Int]) : ListBuffer[ListBuffer[(List[Double], String)]] =  {
    val copy_cluster = collection.mutable.ListBuffer.empty[ListBuffer[(List[Double], String)]]
    current_cluster.copyToBuffer(copy_cluster)
    val listBuffer_adding = collection.mutable.ListBuffer.empty[(List[Double], String)]
    for (number <- listBuffer_int) {
      val listBuffer_tmp = current_cluster.apply(number)
      for (elem <- listBuffer_tmp) {
        listBuffer_adding.append(elem)
      }
    }

    for (number <- listBuffer_int.sorted) {
      current_cluster -= copy_cluster.apply(number)
    }

    current_cluster.append(listBuffer_adding.sortBy(elem => elem._2.size))
    current_cluster
  }

  def Count(list : ListBuffer[(List[Double], String)]) : (ListBuffer[String], ListBuffer[String]) = {
    var header = list.head._2
    val list1 = collection.mutable.ListBuffer.empty[String]
    val list2 = collection.mutable.ListBuffer.empty[String]

    for (sub_list <- list) {
      if (sub_list._2.compareTo(header) != 0) {
        list1.append(sub_list._2)
      } else {
        list2.append(sub_list._2)
      }
    }

    if (list1.size > list2.size) {
      (list2, list1)
    } else {
      (list1, list2)
    }

  }


  def WriteFile(current_cluster : ListBuffer[ListBuffer[(List[Double], String)]], list_count : ListBuffer[(ListBuffer[String], ListBuffer[String])]) : Unit = {
    val file = new File("./OutputFiles")
    file.mkdir()
    val out = new BufferedWriter(new FileWriter(new File("./OutputFiles/Yaosheng_Zhang.txt")))
    out.write("")
    out.flush()
    var apply_number = 0
    var total_wrong = 0
    for (list <- current_cluster) {
      print("cluster:" + list_count.apply(apply_number)._2.head + '\n')
      out.append("cluster:" + list_count.apply(apply_number)._2.head + '\n')

      for (element <- list) {
        print("[")
        out.append("[")
        for (number <- element._1) {
          print(number.toString + ", ")
          out.append(number.toString + ", ")
        }
        print("'" + element._2 + "'")
        out.append("'" + element._2 + "'")

        print("]" + '\n')
        out.append("]" + '\n')
      }

      print("Number of points in this cluster:" + (list_count.apply(apply_number)._2.size + list_count.apply(apply_number)._1.size) + "\n\n")
      out.append("Number of points in this cluster:" + (list_count.apply(apply_number)._2.size + list_count.apply(apply_number)._1.size) + "\n\n")
      total_wrong += list_count.apply(apply_number)._1.size
      apply_number += 1
    }

    print("Number of points wrongly assigned:" + total_wrong + '\n')
    out.append("Number of points wrongly assigned:" + total_wrong)
    out.flush()
    out.close()
  }
}
