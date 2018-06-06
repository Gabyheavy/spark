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
    * @param k reservoir size
    * @param seed random seed
    * @return (samples, input size)
    */
  def reservoirListSampleAndCount[T : ClassTag](
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

  def updateVariableAndCouple1
  (listCouple: ListBuffer[List[Int]], listVariable: ListBuffer[Int]
   , coupleAddedToTree: List[Int], listSelectedVariables: ListBuffer[Int]): Unit = {
    listVariable -= coupleAddedToTree(0)

    def defineCouple(nbrVariable: Int): ListBuffer[List[Int]] = {
      var listCouple = ListBuffer[List[Int]]()
      for (i <- 1 to nbrVariable) {
        for (j <- i + 1 to nbrVariable) {
          listCouple = listCouple :+ (i :: j :: Nil)
        }

      }
      return listCouple
    }
  }

  def defineVariable(nbrVariable: Int): ListBuffer[Int] = {
    var listVariable = ListBuffer[Int]()
    for (i <- 1 to nbrVariable) {
      listVariable = listVariable :+ i
    }
    return listVariable
  }

  def updateVariableAndCouple1(listCouple: ListBuffer[List[Int]], listVariable: ListBuffer[Int], coupleAddedToTree: List[Int], listSelectedVariables: ListBuffer[Int]): Unit = {
    listVariable -= coupleAddedToTree(0)
    listVariable -= coupleAddedToTree(1)
    for (i <- 0 to listSelectedVariables.length - 1){
      listCouple -= List(listSelectedVariables(i), coupleAddedToTree(0))
      listCouple -= List(coupleAddedToTree(0), listSelectedVariables(i))
      listCouple -= List(listSelectedVariables(i), coupleAddedToTree(1))
      listCouple -= List(coupleAddedToTree(1), listSelectedVariables(i))
    }
  }

  def updateVariableAndCouple2 (listCouple : ListBuffer[List[Int]], listVariable: ListBuffer[Int], variableAddedToTree : Int,  listSelectedVariables: ListBuffer[Int]) : Unit ={
    listVariable -= variableAddedToTree
    for (i <- 0 to listSelectedVariables.length - 1){
      listCouple -= List(listSelectedVariables(i), variableAddedToTree)
      listCouple -= List(variableAddedToTree, listSelectedVariables(i))
    }
    listVariable -= coupleAddedToTree(1)
    for ( i <- 0 to listSelectedVariables.length - 1 ){
      listCouple -= List(listSelectedVariables(i), coupleAddedToTree(0))
      listCouple -= List(coupleAddedToTree(0), listSelectedVariables(i))
      listCouple -= List(listSelectedVariables(i), coupleAddedToTree(1))
      listCouple -= List(coupleAddedToTree(1), listSelectedVariables(i))
    }
  }

  def updateVariableAndCouple2 (listCouple : ListBuffer[List[Int]], listVariable: ListBuffer[Int], variableAddedToTree : Int,  listSelectedVariables: ListBuffer[Int]) : Unit = {
    listVariable -= variableAddedToTree
    for (i <- 0 to listSelectedVariables.length - 1) {
      listCouple -= List(listSelectedVariables(i), variableAddedToTree)
      listCouple -= List(variableAddedToTree, listSelectedVariables(i))
    }
  }


  def updateSelectedVariable1 (coupleAddedToTree: List[Int], listSelectedBinary: ListBuffer[Int], listSelectedVariables : ListBuffer[Int]) : Unit ={
    //println(coupleAddedToTree)
    listSelectedBinary(coupleAddedToTree(0)-1) = 1
    listSelectedBinary(coupleAddedToTree(1)-1) = 1
    if(!listSelectedVariables.contains(coupleAddedToTree(0))){
      listSelectedVariables += coupleAddedToTree(0)
    }
    if(!listSelectedVariables.contains(coupleAddedToTree(1))){
      listSelectedVariables += coupleAddedToTree(1)
    }
    //println(listSelectedVariables)
  }

  def updateSelectedVariable2 (variableAddedToTree : Int, listSelectedBinary: ListBuffer[Int], listSelectedVariables : ListBuffer[Int]) : Unit ={
    listSelectedBinary(variableAddedToTree-1) = 1
    if(!listSelectedVariables.contains(variableAddedToTree)){
      listSelectedVariables += variableAddedToTree
    }
  }


  /**
    *
    * @param nbrVariable
    * @param nbrTree
    * @param nbrVarPerTree
    * @return
    */
  def constructionTree(nbrVariable: Int, nbrTree: Int, nbrVarPerTree: Int): List[Array[Int]] = {

    var listCouple: ListBuffer[List[Int]] = defineCouple(nbrVariable)
    var listVariable: ListBuffer[Int] = defineVariable(nbrVariable)

    val rand = new Random(System.currentTimeMillis())

    var resTreeForestVariables = List[Array[Int]]()
    var resTreeForestBinary = List[Array[Int]]()

    for (i <- 1 to nbrTree) {
      var resOneTreeVariables = ListBuffer[Int]()
      var resOneTreeBinary = ListBuffer[Int]()
      for (i <- 1 to nbrVariable) {
        resOneTreeBinary = resOneTreeBinary :+ 0
      }

      listVariable = defineVariable(nbrVariable)

      for (j <- 1 to nbrVarPerTree) {

        while (resOneTreeVariables.length < nbrVarPerTree) {
          // println("variablePossible: " + listVariable)
          // println("CouplePossible: " + listCouple)
          if (listCouple.length == 0) {
            listCouple = defineCouple(nbrVariable)
          }
          if (resOneTreeVariables.length < nbrVarPerTree - 1) {
            val random_index = rand.nextInt(listCouple.length)
            val variableToAdd = listCouple(random_index)
            updateSelectedVariable1(variableToAdd, resOneTreeBinary, resOneTreeVariables)
            updateVariableAndCouple1(listCouple, listVariable, variableToAdd, resOneTreeVariables)
          }
          if (resOneTreeVariables.length == nbrVarPerTree - 1) {
            val random_index = rand.nextInt(listVariable.length)
            val variableToAdd = listVariable(random_index)
            updateSelectedVariable2(variableToAdd, resOneTreeBinary, resOneTreeVariables)
            updateVariableAndCouple2(listCouple, listVariable, variableToAdd, resOneTreeVariables)
          }
        }

      }
      resTreeForestVariables = resTreeForestVariables :+
        resOneTreeVariables.toArray // To be conform with spark random forest
      resTreeForestBinary = resTreeForestBinary :+ resOneTreeBinary.toArray
    }

    // println (testRepartionVariable(nbrVariable,resTreeForestBinary))

    return resTreeForestVariables

  }

  /*
   */
  def testRepartionVariable(
                             nbrVariable: Int,
                             treeForestBinary : List[Array[Int]])
  : ListBuffer[Int] = {
    var res = ListBuffer[Int]()

    for (i <- 1 to nbrVariable) {
      res = res :+ 0
    }
    for (i <- 0 to nbrVariable - 1) {
      for (j <- 0 to treeForestBinary.length - 1) {
        res(i) = res(i) + treeForestBinary(j)(i)
      }
    }

    return res
  }

}
