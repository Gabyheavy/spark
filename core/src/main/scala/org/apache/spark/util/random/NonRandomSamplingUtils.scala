/*

 *
 * OUR NEW FILE
 *
 */


package org.apache.spark.util.random

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.Random

private[spark] object NonRandomSamplingUtils {


  /**
    * Reservoir list sampling implementation
    * that make sure to reduce variance of variables occurrences.
    * Also returns the input size.
    *
    * @param input input size
    * @param k     reservoir size
    * @param seed  random seed
    * @return (samples, input size)
    */
  def reservoirListSampleAndCount[T: ClassTag](
                                                input: Iterator[T],
                                                k: Int,
                                                seed: Long = Random.nextLong())
  : (Array[Array[T]], Long) = {
    (Array(Array()), 0)
  }


  def defineCouple(nbrVariable: Int): ListBuffer[List[Int]] = {
    var listCouple = ListBuffer[List[Int]]()
    for (i <- 1 to nbrVariable) {
      for (j <- i + 1 to nbrVariable) {
        listCouple = listCouple :+ (i :: j :: Nil)
      }

    }
    return listCouple
  }

  def defineVariable(nbrVariable: Int): ListBuffer[Int] = {
    var listVariable = ListBuffer[Int]()
    for (i <- 1 to nbrVariable) {
      listVariable = listVariable :+ i
    }
    return listVariable
  }

  def updateCouple1(listCouple: ListBuffer[List[Int]], coupleAddedToTree: List[Int],
                    listSelectedVariables: ListBuffer[Int]): Unit = {
    for (i <- 0 to listSelectedVariables.length - 1) {
      listCouple -= List(listSelectedVariables(i), coupleAddedToTree(0))
      listCouple -= List(coupleAddedToTree(0), listSelectedVariables(i))
      listCouple -= List(listSelectedVariables(i), coupleAddedToTree(1))
      listCouple -= List(coupleAddedToTree(1), listSelectedVariables(i))
    }
  }

  def updateCouple2(listCouple: ListBuffer[List[Int]], variableAddedToTree: Int,
                    listSelectedVariables: ListBuffer[Int]): Unit = {
    for (i <- 0 to listSelectedVariables.length - 1) {
      listCouple -= List(listSelectedVariables(i), variableAddedToTree)
      listCouple -= List(variableAddedToTree, listSelectedVariables(i))
    }
  }

  def updateSelectedVariable1(coupleAddedToTree: List[Int], listSelectedBinary: ListBuffer[Int],
                              listSelectedVariables: ListBuffer[Int],
                              cumSelectedVariables: ListBuffer[Int]): Unit = {
    listSelectedBinary(coupleAddedToTree(0) - 1) = 1
    listSelectedBinary(coupleAddedToTree(1) - 1) = 1
    if (!listSelectedVariables.contains(coupleAddedToTree(0))) {
      listSelectedVariables += coupleAddedToTree(0)
      cumSelectedVariables(coupleAddedToTree(0) - 1) =
        cumSelectedVariables(coupleAddedToTree(0) - 1) + 1
    }
    if (!listSelectedVariables.contains(coupleAddedToTree(1))) {
      listSelectedVariables += coupleAddedToTree(1)
      cumSelectedVariables(coupleAddedToTree(1) - 1) =
        cumSelectedVariables(coupleAddedToTree(1) - 1) + 1
    }
  }

  def updateSelectedVariable2(variableAddedToTree: Int, listSelectedBinary: ListBuffer[Int],
                              listSelectedVariables: ListBuffer[Int],
                              cumSelectedVariables: ListBuffer[Int]): Unit = {
    listSelectedBinary(variableAddedToTree - 1) = 1
    if (!listSelectedVariables.contains(variableAddedToTree)) {
      listSelectedVariables += variableAddedToTree
      cumSelectedVariables(variableAddedToTree - 1) =
        cumSelectedVariables(variableAddedToTree - 1) + 1
    }
  }

  def constructionTree(nbrVariable: Int, nbrTree: Int, nbrVarPerTree: Int): List[Array[Int]] = {

    var listCouple: ListBuffer[List[Int]] = defineCouple(nbrVariable)
    val listVariable: ListBuffer[Int] = defineVariable(nbrVariable)
    val rand = new Random(System.currentTimeMillis())

    var resTreeForestVariables = List[Array[Int]]()
    var resTreeForestBinary = List[Array[Int]]()
    var resCumSelectedVariables = ListBuffer[Int]()

    for (i <- 1 to nbrVariable) {
      resCumSelectedVariables = resCumSelectedVariables :+ 0
    }

    for (i <- 1 to nbrTree) {
      var resOneTreeVariables = ListBuffer[Int]()
      var resOneTreeBinary = ListBuffer[Int]()
      for (i <- 1 to nbrVariable) {
        resOneTreeBinary = resOneTreeBinary :+ 0
      }


      for (j <- 1 to nbrVarPerTree) {

        while (resOneTreeVariables.length < nbrVarPerTree) {

          if (listCouple.length == 0) {
            listCouple = defineCouple(nbrVariable)
          }

          if (resOneTreeVariables.length < nbrVarPerTree - 1) {
            val random_index = rand.nextInt(listCouple.length)
            val variableToAdd = listCouple(random_index)
            updateSelectedVariable1(variableToAdd, resOneTreeBinary,
              resOneTreeVariables, resCumSelectedVariables)
            updateCouple1(listCouple, variableToAdd, resOneTreeVariables)
          }

          if (resOneTreeVariables.length == nbrVarPerTree - 1) {
            var copyResCumSelctedVariables = resCumSelectedVariables
            var index = copyResCumSelctedVariables.zipWithIndex.min._2
            var variableToAdd = listVariable(index)
            while (resOneTreeVariables.contains(variableToAdd)) {
              copyResCumSelctedVariables(index) = 0
              index = copyResCumSelctedVariables.zipWithIndex.min._2
              variableToAdd = listVariable(index)
            }
            updateSelectedVariable2(variableToAdd, resOneTreeBinary,
              resOneTreeVariables, resCumSelectedVariables)
            updateCouple2(listCouple, variableToAdd, resOneTreeVariables)
          }

        }
      }

      // println(resOneTreeVariables.mkString(" "))

      resTreeForestVariables = resTreeForestVariables :+
        resOneTreeVariables.toArray // To be conform with spark random forest
      resTreeForestBinary = resTreeForestBinary :+ resOneTreeBinary.toArray
    }

    // println(resCumSelectedVariables)

    return resTreeForestVariables

  }

}
