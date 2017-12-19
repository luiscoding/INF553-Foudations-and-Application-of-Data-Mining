import org.apache.spark._
import scala.collection.mutable
import scala.collection.mutable.Queue
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set
import java.io._
import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode


object Yaosheng_Zhang {
  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
    val inputfilename = args{0}
    val outputcommunities = args{1}
    val outputBetwness = args{2}

    val conf = new SparkConf().setAppName("yaoshengzhang").setMaster("local[*]")
    val sc = new SparkContext(conf)
    var data = sc.textFile(inputfilename)
    val firstLine = data.first()
    data = data.filter(elem => elem != firstLine)
    val text = data.map(elem => (elem.split(','){0}.toLong, elem.split(','){1}.toLong))

    val movie = text.groupByKey().sortByKey().map(elem => (elem._1, elem._2.toSet)).toLocalIterator.toList

    var i = 0
    var j = 1

    //val vertices = collection.mutable.Set.empty[(Long, Int)]
    val vertices = collection.mutable.Set.empty[Long]
    val edges = collection.mutable.Set.empty[(Long, Long)]

    while (i < movie.size) {
      j = i + 1
      while (j < movie.size) {
        val tmpSet = movie.apply(i)._2 intersect movie.apply(j)._2
        if (tmpSet.size >= 3) {
          vertices.add(j + 1)
          vertices.add( i + 1)
          edges.add(i + 1, j + 1)
          edges.add(j + 1, i + 1)
        }
        j += 1
      }
      i += 1
    }

    val verticesRDD = sc.parallelize(vertices.toList).collect().toList
    val edgesRdd = sc.parallelize(edges.toList).collect().toList
    //            val verticesRDD = sc.parallelize(List(0L, 1L, 2L, 3L, 4L, 5L, 6L)).collect().toList
    //            val edgesRdd = sc.parallelize(List((0L, 1L), (1L, 0L),
    //                (0L,2L), (2L, 0L), (1L, 2L), (2L, 1L), (1L, 3L), (3L, 1L), (3L, 4L), (4L, 3L),
    //                    (3L, 6L), (6L, 3L), (3L, 5L), (5L, 3L), (4L, 5L), (5L, 4L), (6L, 5L) , (5L, 6L))).collect().toList

    val edgesMap : Map[Long, ListBuffer[Long]] = Map()
    for (edge <- edgesRdd) {
      if (edgesMap.contains(edge._1)) {
        edgesMap.apply(edge._1) += edge._2
      } else {
        val listBuffer = ListBuffer.empty[Long]
        listBuffer += edge._2
        edgesMap += (edge._1 -> listBuffer)
      }
    }

    val betweeness = computeBetweeness(outputcommunities, verticesRDD, edgesMap)
    val community_ = community(edgesMap, betweeness, edgesRdd, verticesRDD)

    var intg = 0
    val bet = betweeness.map(elem => {
      if (elem._1._2 < elem._1._1) {
        (elem._1.swap, elem._2)
      } else {
        (elem._1, elem._2)
      }
    }).toList.sorted
    val writer = new PrintWriter(new File(outputBetwness))
    for (elem <- bet) {
      val elem1 = elem._1._1
      val elem2 = elem._1._2
      val elem3 = elem._2
      writer.append("(" + elem1 +',' + elem2 + "," + elem3 + ")" + '\n')
    }
    writer.flush
    writer.close()
    for (commu <- community_) {
      
        val writer = new PrintWriter(new File(outputcommunities))
        val commuu = commu.map(elem => elem.toList.sorted).sortWith(compare)
        for (com <- commuu) {
          val list = com.toList.sorted
          val str = list.toString
          val substr = str.substring(5, str.size-1)
          writer.append("[" +substr + ']'+ '\n')
        }
        
      }
      //        val name : String = "community" + intg + ".txt"
      //        val writer = new PrintWriter(new File(name))
      //
      //        val commuu = commu.map(elem => elem.toList.sorted).sortWith(compare)
      //        for (com <- commuu) {
      //            val list = com.toList.sorted
      //            val str = list.toString
      //            val substr = str.substring(5, str.size-1)
      //            writer.append("[" +substr + ']'+ '\n')
      //        }
      intg += 1
      //writer.close()
    }


    val end = System.nanoTime()
    println("Total Costs: " + (end - start) * math.pow(10, -9))
  }

  def compare(l1 : List[Long], l2: List[Long])  : Boolean = {
    l1.head < l2.head
  }

  def constructTree(verticesRDD : List[Long], edgesMap : Map[Long, ListBuffer[Long]], root : Long) : (Map[Long, ListBuffer[Long]], Map[Long, Int], Map[Long, Int]) =  {
    val depthMap : Map[Long, Int] = Map()
    val labelMap : Map[Long, Int] = Map()
    val tree : Map[Long, ListBuffer[Long]] = Map()
    for (vertex <- verticesRDD) {
      depthMap += (vertex -> -10)
      labelMap += (vertex -> 0)
    }
    depthMap.update(root, 0)
    labelMap.update(root, 1)
    val queue : mutable.Queue[Long] = Queue()
    queue.enqueue(root)


    while (queue.nonEmpty) {
      val front = queue.dequeue()
      val frontDepth = depthMap.apply(front)
      val listBuf = ListBuffer.empty[Long]

      val successors = edgesMap.apply(front)
      for (successor <- successors) {
        if (depthMap.apply(successor) > depthMap.apply(front) || depthMap.apply(successor) == -10) {
          if (!queue.contains(successor)) {
            queue.enqueue(successor)
          }
          labelMap.update(successor, labelMap.apply(successor) + 1)
          depthMap.update(successor, frontDepth + 1)
          listBuf += successor
        }
      }
      tree += (front -> listBuf)
    }

    (tree, labelMap, depthMap)
  }

  def reverseTree(Tree : Map[Long, ListBuffer[Long]]) : Map[Long, ListBuffer[Long]]= {
    val TreeReversed: Map[Long, ListBuffer[Long]] = Map()
    for (subMap <- Tree) {
      for (dstVertex <- subMap._2) {
        if(TreeReversed.contains(dstVertex)) {
          TreeReversed.apply(dstVertex).append(subMap._1)
        } else {
          val listBuffer = ListBuffer.empty[Long]
          listBuffer.append(subMap._1)
          TreeReversed += (dstVertex -> listBuffer)
        }
      }
    }
    TreeReversed
  }

  def computeCredit(tree : Map[Long, ListBuffer[Long]], labelMap : Map[Long, Int], root : Long): (Map[Long, Double], Map[(Long, Long), Double]) = {
    val edgeMap : Map[(Long, Long), Double] = Map()
    val vertexMap : Map[Long, Double] = Map()

    val queue : mutable.Queue[Long] = Queue()
    val reversedTree = reverseTree(tree)

    for (subRevTree <- reversedTree) {
      vertexMap += (subRevTree._1 -> 1.0)
      for (dst <- subRevTree._2) {
        edgeMap += ((subRevTree._1, dst) -> 0.0)
      }
    }
    vertexMap.update(root, 0.0)

    // find all leaf nodes and enqueue them
    for (subTree <- tree) {
      if (subTree._2.isEmpty) {
        queue.enqueue(subTree._1)
      }
    }
    //println(queue)
    while (queue.nonEmpty) {
      val front = queue.dequeue()
      var fathersBuffer = ListBuffer.empty[Long]
      if (front != root) {
        fathersBuffer = reversedTree.apply(front)
      }
      for (father <- fathersBuffer) {
        if(father != root) {}
        val shortestPathtoFather =  labelMap.apply(father).toDouble
        val shortesPathtoFront = labelMap.apply(front).toDouble
        val ratio = shortestPathtoFather / shortesPathtoFront
        val frontValue = ratio * vertexMap.apply(front)
        edgeMap.update((front, father), frontValue + edgeMap.apply((front, father)))
        vertexMap.update(father, frontValue + vertexMap.apply(father))
        if (!queue.contains(father)) {
          queue.enqueue(father)
        }
      }
    }
    //        println(edgeMap)
    //        edgeMap.foreach(println)
    //        println(vertexMap)
    //        vertexMap.foreach(println)
    (vertexMap, edgeMap)
  }

  def computeBetweeness1(verticesRDD : List[Long], edgesMap : Map[Long, ListBuffer[Long]]): Map[(Long, Long), Double] = {
    var betweenessMap: Map[(Long, Long), Double] = Map()
    val roots = verticesRDD.sorted
    for (root <- roots) {


      val treeValue = constructTree(verticesRDD, edgesMap, root)

      val tree = treeValue._1
      val labelMap = treeValue._2
      val credit = computeCredit(tree, labelMap, root)
      val edgeMap = credit._2
      for (subEdgeMap <- edgeMap) {
        val vertex = subEdgeMap._1
        if (betweenessMap.contains(vertex)) {
          betweenessMap.update(vertex, betweenessMap.apply(vertex) + subEdgeMap._2)
        } else if (betweenessMap.contains(vertex.swap)) {
          betweenessMap.update(vertex.swap, betweenessMap.apply(vertex.swap) + subEdgeMap._2)
        } else {
          betweenessMap += (vertex->subEdgeMap._2)
        }
      }
    }

    val bet = betweenessMap.map(elem => {
      val value = (elem._2 / 2).formatted("%.1f").toDouble
      if (elem._1._2 < elem._1._1) {
        (elem._1.swap, value)
      } else {
        (elem._1, value)
      }
    }).toList.sorted
    val writer = new PrintWriter(new File("Yaosheng_Zhang_betweenness.txt" ))
    for (elem <- bet) {
      val elem1 = elem._1._1
      val elem2 = elem._1._2
      val elem3 = elem._2
      writer.append("(" + elem1 +',' + elem2 + "," + elem3 + ")" + '\n')
    }
    writer.flush
    writer.close()
    betweenessMap
  }

  def computeBetweeness(str:String, verticesRDD : List[Long], edgesMap : Map[Long, ListBuffer[Long]]): Map[(Long, Long), BigDecimal] = {
    var betweenessMap: Map[(Long, Long), Double] = Map()
    var betweenessMap1: Map[(Long, Long), BigDecimal] = Map()
    val roots = verticesRDD.sorted
    for (root <- roots) {

      val treeValue = constructTree(verticesRDD, edgesMap, root)
      val tree = treeValue._1
      val labelMap = treeValue._2
      val credit = computeCredit(tree, labelMap, root)
      val edgeMap = credit._2
      for (subEdgeMap <- edgeMap) {
        val vertex = subEdgeMap._1
        if (betweenessMap.contains(vertex)) {
          betweenessMap.update(vertex, betweenessMap.apply(vertex) + subEdgeMap._2)
        } else if (betweenessMap.contains(vertex.swap)) {
          betweenessMap.update(vertex.swap, betweenessMap.apply(vertex.swap) + subEdgeMap._2)
        } else {
          betweenessMap += (vertex->subEdgeMap._2)
        }
      }
    }
    for (elem <- betweenessMap) {

      betweenessMap1 += (elem._1->BigDecimal(elem._2 / 2).setScale(1, RoundingMode.DOWN))
    }
    //    val bet = betweenessMap1.map(elem => {
    //      if (elem._1._2 < elem._1._1) {
    //        (elem._1.swap, elem._2)
    //      } else {
    //        (elem._1, elem._2)
    //      }
    //    }).toList.sorted
    //    val writer = new PrintWriter(new File(str))
    //    for (elem <- bet) {
    //      val elem1 = elem._1._1
    //      val elem2 = elem._1._2
    //      val elem3 = elem._2
    //      writer.append("(" + elem1 +',' + elem2 + "," + elem3 + ")" + '\n')
    //    }
    //    writer.flush
    //    writer.close()
    betweenessMap1
  }


  def community1(edgesMap : Map[Long, ListBuffer[Long]], betweenessMap: Map[(Long, Long), Double], edgesRdd : List[(Long, Long)], verticesRDD : List[Long]): ListBuffer[ListBuffer[mutable.Set[Long]]] = {
    val Qlist : mutable.ListBuffer[Double] = ListBuffer()
    var total = edgesRdd.size
    val communityList : ListBuffer[ListBuffer[mutable.Set[Long]]] = ListBuffer.empty[ListBuffer[mutable.Set[Long]]]
    val swap = betweenessMap.toList
    val sorted = swap.sortBy(elem => elem._2).reverse
    val betweenessList = sorted.map(elem => elem._2).distinct

    val mapSort = sorted.groupBy(elem => elem._2)


    for (betweness <- betweenessList) {
      val sortList = mapSort.apply(betweness)
      val edgeNeed = sortList.map(elem => elem._1)
      val vertexSet = verticesRDD.to[collection.mutable.Set]

      var symbolVer = vertexSet.head
      for (edge <- edgeNeed) {
        val vertexA = edge._1
        val vertexB = edge._2
        val listBufA = edgesMap.apply(vertexA).filter(elem => elem != vertexB)
        val listBufB = edgesMap.apply(vertexB).filter(elem => elem != vertexA)
        edgesMap.update(vertexA, listBufA)
        edgesMap.update(vertexB, listBufB)
        total -= 2
        symbolVer = vertexA
      }

      val connectedComponentA = FindConnectedComponent(edgesMap, symbolVer, verticesRDD)

      val allComponent = FindAllComponent(vertexSet, edgesMap, connectedComponentA, verticesRDD)
      println(allComponent.size)
      communityList.append(allComponent)

      val Q = modularity(allComponent, edgesMap, total)
      Qlist.append(Q)

    }
    communityList
  }

  def community(edgesMap : Map[Long, ListBuffer[Long]], betweenessMap: Map[(Long, Long), BigDecimal], edgesRdd : List[(Long, Long)], verticesRDD : List[Long]): ListBuffer[ListBuffer[mutable.Set[Long]]] = {
    val Qlist : mutable.ListBuffer[Double] = ListBuffer()
    var total = edgesRdd.size
    val communityList : ListBuffer[ListBuffer[mutable.Set[Long]]] = ListBuffer.empty[ListBuffer[mutable.Set[Long]]]
    val swap = betweenessMap.toList
    val sorted = swap.sortBy(elem => elem._2).reverse
    val betweenessList = sorted.map(elem => elem._2).distinct

    val mapSort = sorted.groupBy(elem => elem._2)

    for (betweness <- betweenessList) {

      val sortList = mapSort.apply(betweness)
      val edgeNeed = sortList.map(elem => elem._1)
      val vertexSet = verticesRDD.to[collection.mutable.Set]
      var symbolVer = vertexSet.head
      for (edge <- edgeNeed) {
        val vertexA = edge._1
        val vertexB = edge._2
        val listBufA = edgesMap.apply(vertexA).filter(elem => elem != vertexB)
        val listBufB = edgesMap.apply(vertexB).filter(elem => elem != vertexA)
        edgesMap.update(vertexA, listBufA)
        edgesMap.update(vertexB, listBufB)
        total -= 2
        symbolVer = vertexA
      }

      val connectedComponentA = FindConnectedComponent(edgesMap, symbolVer, verticesRDD)

      val allComponent = FindAllComponent(vertexSet, edgesMap, connectedComponentA, verticesRDD)
      //println(allComponent.size)
      communityList.append(allComponent)

      val Q = modularity(allComponent, edgesMap, total)
      Qlist.append(Q)

    }
    communityList
  }

  def FindConnectedComponent(edgesMap : Map[Long, ListBuffer[Long]], vertexA : Long, verticesRDD : List[Long]): mutable.Set[Long] = {
    //println(edgesMap)
    val connectedPart : mutable.Set[Long] = Set()
    if (edgesMap.apply(vertexA).isEmpty) {
      connectedPart.add(vertexA)
    }
    val labeledMap : Map[Long, Boolean] = Map()

    val queue : mutable.Queue[Long] = Queue()
    for (vertex <- verticesRDD) {
      labeledMap += (vertex -> false)
    }
    queue.enqueue(vertexA)
    while(queue.nonEmpty) {
      val front = queue.dequeue()
      val successors = edgesMap.apply(front)
      for (successor <- successors) {
        if (labeledMap.apply(successor) == false) {
          queue.enqueue(successor)
          labeledMap.update(successor, true)
        }
        connectedPart.add(successor)
      }
    }
    connectedPart
  }

  def FindAllComponent(vertexSet : mutable.Set[Long], edgesMap : Map[Long, ListBuffer[Long]], connectedComponent: mutable.Set[Long], verticesRDD : List[Long]): mutable.ListBuffer[mutable.Set[Long]] = {
    val ret : mutable.ListBuffer[mutable.Set[Long]] = ListBuffer.empty[mutable.Set[Long]]
    var rest = vertexSet.diff(connectedComponent)
    //println("connectedComponent = " + connectedComponent)
    ret.append(connectedComponent)
    //println(rest)
    while (rest.nonEmpty) {
      val vertex = rest.headOption
      if (vertex.isDefined) {
        val connected = FindConnectedComponent(edgesMap, vertex.get, verticesRDD)
        rest = rest.diff(connected)
        ret.append(connected)
      }
    }
    ret
  }

  def modularity(allComponent : mutable.ListBuffer[mutable.Set[Long]], edgesMap : Map[Long, ListBuffer[Long]], total :Int): Double = {
    var Q : Double = 0.0
    var num: Double = 0.0
    val edgesTreeSet : Map[Long, collection.mutable.TreeSet[Long]] = Map()

    for (edge <- edgesMap) {
      edgesTreeSet += (edge._1 ->edge._2.to[collection.mutable.TreeSet])
    }

    for (connectedComponent <- allComponent) {

      //println("totalEdgesInCommunity : " + totalEdgesInCommunity)
      val vertexList= connectedComponent.to[ListBuffer].sorted
      val combination : mutable.ListBuffer[(Long, Long)] = ListBuffer.empty[(Long, Long)]
      var i = 0
      var j = 0
      while (i < vertexList.size ) {
        j = i + 1
        while (j < vertexList.size) {
          combination.append((vertexList.apply(i), vertexList.apply(j)))
          j += 1
        }
        i += 1
      }

      for (tuple <- combination) {
        val src = tuple._1
        val des = tuple._2
        val kSrc = edgesMap.apply(src).size
        val kDes = edgesMap.apply(des).size

        if (edgesTreeSet.apply(src).contains(des)) {
          num += (1.0 - (( kSrc * kDes).toDouble / total.toDouble))
          //println("   num += (1.0 - ( kSrc * kDes).toDouble / total.toDouble) = " + num)


        } else {
          num += (0.0 - ((kSrc.toDouble * kDes.toDouble) / total.toDouble))
          //println("   num += (0.0 - ( kSrc * kDes).toDouble / total.toDouble) = " + num)

        }
      }
    }
    Q = num / total

    Q
  }
}

