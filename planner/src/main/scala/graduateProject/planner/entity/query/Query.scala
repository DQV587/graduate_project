package graduateProject.planner.entity.query

import graduateProject.planner.entity.expression.Comparison
import graduateProject.planner.entity.hypergraph.relationHypergraph.{Relation, Variable}

import scala.collection.mutable

case class Query(relations:List[Relation], comparisons:List[Comparison], output:List[Variable], isFull:Boolean) {
  override def toString: String = {
    val builder=new mutable.StringBuilder()
    builder.append("Relations:\r\n")
    for(relation<-relations){
      builder.append("\t").append(relation.toString).append("\r\n")
    }
    builder.append("Comparisons:\r\n")
    for(comparison<-comparisons){
      builder.append("\t").append(comparison.toString).append("\r\n")
    }
    builder.append("output variables:\r\n")
    builder.append("\t")
    for(variable<-output){
      builder.append(variable.toString).append(" ")
    }
    builder.append("\r\n")
    builder.toString()
  }
}
