package graduateProject.planner.algorithm.planGenerator

import graduateProject.planner.entity.hypergraph.comparisonHypergraph.{ComparisonHyperGraph, ReduceInformation}
import graduateProject.planner.entity.hypergraph.relationHypergraph.{AggregatedRelation, TableScanRelation}

import scala.collection.mutable

object ReducePlanGenerator {
  def apply(comparisonHyperGraph: ComparisonHyperGraph):List[ReduceInformation]={
    val reduceInformationArray = mutable.ArrayBuffer[ReduceInformation]()
    val nodeNum = comparisonHyperGraph.nodeSet.size
    for (i <- 0 to nodeNum) {
      val reducibleRelationSet = comparisonHyperGraph.getReducibleRelations
      val nextReduceRelation = {
        if (reducibleRelationSet.forall(relation => relation.isInstanceOf[TableScanRelation]))
          reducibleRelationSet.head
        else
          reducibleRelationSet.filter(relation => relation.isInstanceOf[AggregatedRelation]).head
      }
      val information = comparisonHyperGraph.reduceRelation(nextReduceRelation)
      reduceInformationArray.append(information)
    }
    reduceInformationArray.toList
  }
}

case class ReduceState(informationList:List[ReduceInformation],chg:ComparisonHyperGraph)

