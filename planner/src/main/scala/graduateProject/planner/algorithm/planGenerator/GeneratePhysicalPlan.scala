package graduateProject.planner.algorithm.planGenerator

import graduateProject.parser.CatalogManager
import graduateProject.planner.entity.hypergraph.comparisonHypergraph.{ComparisonHyperGraph, ReduceInformation}

import scala.collection.mutable

object GeneratePhysicalPlan {
  def apply(catalog:CatalogManager,comparisonHyperGraph: ComparisonHyperGraph):PhysicalPlan={

    val reduceInformationArray=mutable.ArrayBuffer[ReduceInformation]()
    val nodeNum=comparisonHyperGraph.nodeSet.size
    for(i <-0 to nodeNum){
      val reducibleRelationSet=comparisonHyperGraph.getReducibleRelations
      val information=comparisonHyperGraph.reduceRelation(reducibleRelationSet.head)
      reduceInformationArray.append(information)
    }
    reduceInformationArray.foreach(x=>println(x))
    PhysicalPlan()
  }
}

case class PhysicalPlanState(informationList:List[ReduceInformation],chg:ComparisonHyperGraph)
