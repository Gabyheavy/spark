/*

 *
 * OUR NEW FILE
 *
 */


package org.apache.spark.util.random

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.reflect.ClassTag
import scala.util.Random

private[spark] object NonRandomSamplingUtils{


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


  //Function to calculate possible couple
  //Parameter: number of variables
  //Return: list of couples
  def defineCouple(nbrVariable: Int): ListBuffer[List[Int]] = {
    var listCouple = ListBuffer[List[Int]]()
    for (i <- 0 to nbrVariable - 1) {
      for (j <- i + 1 to nbrVariable - 1) {
        listCouple = listCouple :+ (i :: j :: Nil)
      }

    }
    return listCouple
  }

  //Function to have a list of all variables
  //Parameter: number of variables
  //Return: list of variables
  def defineVariable(nbrVariable: Int): ListBuffer[Int] = {
    var listVariable = ListBuffer[Int]()
    for (i <- 0 to nbrVariable - 1) {
      listVariable = listVariable :+ i
    }
    return listVariable
  }

  //Function tu update the list of couple and to count the number of couple occurrences in the forest in case of adding a couple of variables
  //Parametes: list of couples, added couple, list of variables, map of couples with corresponding occurrences in the forest
  def updateCouple1(listCouple: ListBuffer[List[Int]], coupleAddedToTree: List[Int],
                    listSelectedVariables: ListBuffer[Int],
                    cumSelectedCoupleVariables: Map[List[Int], Int]): Unit = {
    for (i <- 0 to listSelectedVariables.length - 1) {
      if (listCouple.contains(List(listSelectedVariables(i), coupleAddedToTree(0)))) {
        listCouple -= List(listSelectedVariables(i), coupleAddedToTree(0))
        cumSelectedCoupleVariables(List(listSelectedVariables(i), coupleAddedToTree(0))) =
          cumSelectedCoupleVariables(List(listSelectedVariables(i), coupleAddedToTree(0))) + 1
      }
      if (listCouple.contains(List(coupleAddedToTree(0), listSelectedVariables(i)))) {
        listCouple -= List(coupleAddedToTree(0), listSelectedVariables(i))
        cumSelectedCoupleVariables(List(coupleAddedToTree(0), listSelectedVariables(i))) =
          cumSelectedCoupleVariables(List(coupleAddedToTree(0), listSelectedVariables(i))) + 1
      }
      if (listCouple.contains(List(listSelectedVariables(i), coupleAddedToTree(1)))) {
        listCouple -= List(listSelectedVariables(i), coupleAddedToTree(1))
        cumSelectedCoupleVariables(List(listSelectedVariables(i), coupleAddedToTree(1))) =
          cumSelectedCoupleVariables(List(listSelectedVariables(i), coupleAddedToTree(1))) + 1
      }
      if (listCouple.contains(List(coupleAddedToTree(1), listSelectedVariables(i)))) {
        listCouple -= List(coupleAddedToTree(1), listSelectedVariables(i))
        cumSelectedCoupleVariables(List(coupleAddedToTree(1), listSelectedVariables(i))) =
          cumSelectedCoupleVariables(List(coupleAddedToTree(1), listSelectedVariables(i))) + 1
      }
    }
  }

  //Function tu update the list of couple and to count the number of couple occurrences in the forest in case of adding a variable
  //Parametes: list of couples, added couple, list of variables, map of couples with corresponding occurrences in the forest
  def updateCouple2(listCouple: ListBuffer[List[Int]], variableAddedToTree: Int,
                    listSelectedVariables: ListBuffer[Int],
                    cumSelectedCoupleVariables: Map[List[Int], Int]): Unit = {
    for (i <- 0 to listSelectedVariables.length - 1) {
      if (listCouple.contains(List(listSelectedVariables(i), variableAddedToTree))) {
        listCouple -= List(listSelectedVariables(i), variableAddedToTree)
        cumSelectedCoupleVariables(List(listSelectedVariables(i), variableAddedToTree)) =
          cumSelectedCoupleVariables(List(listSelectedVariables(i), variableAddedToTree)) + 1
      }
      if (listCouple.contains(List(variableAddedToTree, listSelectedVariables(i)))) {
        listCouple -= List(variableAddedToTree, listSelectedVariables(i))
        cumSelectedCoupleVariables(List(variableAddedToTree, listSelectedVariables(i))) =
          cumSelectedCoupleVariables(List(variableAddedToTree, listSelectedVariables(i))) + 1
      }
    }
  }

  //Function tu update the list of variables and to count the number of variable occurrences in the forest in case of adding a couple of variables
  //Parametes: list of couples, added couple, list of variables, map of couples with corresponding occurrences in the forest
  def updateSelectedVariable1(coupleAddedToTree: List[Int], listSelectedBinary: ListBuffer[Int],
                              listSelectedVariables: ListBuffer[Int],
                              cumSelectedVariables: ListBuffer[Int]): Unit = {
    listSelectedBinary(coupleAddedToTree(0)) = 1
    listSelectedBinary(coupleAddedToTree(1)) = 1
    if (!listSelectedVariables.contains(coupleAddedToTree(0))) {
      listSelectedVariables += coupleAddedToTree(0)
      cumSelectedVariables(coupleAddedToTree(0)) =
        cumSelectedVariables(coupleAddedToTree(0)) + 1
    }
    if (!listSelectedVariables.contains(coupleAddedToTree(1))) {
      listSelectedVariables += coupleAddedToTree(1)
      cumSelectedVariables(coupleAddedToTree(1)) =
        cumSelectedVariables(coupleAddedToTree(1)) + 1
    }
  }

  //Function tu update the list of variables and to count the number of variable occurrences in the forest in case of adding a variable
  //Parametes: list of couples, added couple, list of variables, map of couples with corresponding occurrences in the forest
  def updateSelectedVariable2(variableAddedToTree: Int, listSelectedBinary: ListBuffer[Int],
                              listSelectedVariables: ListBuffer[Int],
                              cumSelectedVariables: ListBuffer[Int]): Unit = {
    listSelectedBinary(variableAddedToTree) = 1
    if (!listSelectedVariables.contains(variableAddedToTree)) {
      listSelectedVariables += variableAddedToTree
      cumSelectedVariables(variableAddedToTree) =
        cumSelectedVariables(variableAddedToTree) + 1
    }
  }


  //Principal Function to calculate variables to build each tree in the forest
  //Parameter: number of variables, number of trees in the forest, number of variables per tree
  //Return: list of array of variable indexes (representing a tree)
  def constructionTree(nbrVariable: Int, nbrTree: Int, nbrVarPerTree: Int): List[Array[Int]] = {

    var listCouple: ListBuffer[List[Int]] = defineCouple(nbrVariable)
    val listVariable: ListBuffer[Int] = defineVariable(nbrVariable)
    val rand = new Random(System.currentTimeMillis())

    var resTreeForestVariables = List[Array[Int]]() //List of array of variable indexes
    var resTreeForestBinary = List[Array[Int]]()  //List of array of binary number (1 for taken 0 for not)
    var resCumSelectedVariables = ListBuffer[Int]() //List with the occurrences of each variable
    var resCumSelectedCoupleVariables = Map[List[Int], Int]() //Map with the occurrences of each couple

    for (i <- 1 to nbrVariable) {
      resCumSelectedVariables = resCumSelectedVariables :+ 0
    }

    for (i <- 0 to listCouple.length-1) {
      resCumSelectedCoupleVariables += (listCouple(i) -> 0)
    }

    for (i <- 1 to nbrTree) {
      var resOneTreeVariables = ListBuffer[Int]()
      var resOneTreeBinary = ListBuffer[Int]()
      for (i <- 1 to nbrVariable) {
        resOneTreeBinary = resOneTreeBinary :+ 0
      }

      //For each tree, random selection of couple or variable (if it misses only one variable)
      // Adding variables and updating lists
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
            updateCouple1(listCouple, variableToAdd, resOneTreeVariables,
              resCumSelectedCoupleVariables)
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
            updateCouple2(listCouple, variableToAdd, resOneTreeVariables,
              resCumSelectedCoupleVariables)
          }

        }
      }

      resTreeForestVariables = resTreeForestVariables :+
        resOneTreeVariables.toArray
      resTreeForestBinary = resTreeForestBinary :+ resOneTreeBinary.toArray
    }

    return resTreeForestVariables

  }

}
