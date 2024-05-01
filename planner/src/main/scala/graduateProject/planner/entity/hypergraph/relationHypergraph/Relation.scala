package graduateProject.planner.entity.hypergraph.relationHypergraph

import graduateProject.planner.entity.hypergraph.HyperGraphEdge

/**
 * A relation is a hyperEdge whose nodes are variables.
 */
abstract class Relation extends HyperGraphEdge[Variable] {
  val relationId: Int = Relation.getNewRelationId()

  final def getRelationId(): Int = relationId

  def getTableName(): String

  final def getVariables():Set[Variable]=this.getNodes


  override def hashCode(): Int = getRelationId().##

  override def equals(obj: Any): Boolean = obj match {
    case that: Relation => that.getRelationId() == this.getRelationId()
    case _ => false
  }
}

object Relation {
  var ID = 0

  def getNewRelationId(): Int = {
    ID += 1
    ID
  }
  def newTableScanRelation(tableName: String, variables: Set[Variable]):TableScanRelation=
    TableScanRelation(tableName,variables)

  def newAggregateRelation(tableName: String,variables: Set[Variable],group: List[Int],func: String):AggregatedRelation=
    AggregatedRelation(tableName, variables, group, func)
}
case class AggregatedRelation(val tableName: String, variables: Set[Variable],
                         val group: List[Int], val func: String) extends Relation {
  override def getTableName(): String = tableName

  override def toString: String = {
    val columns = nodeSet.map(n => n.name + ":" + n.dataType).mkString("(", ",", ")")
    val groups = group.mkString("(", ",", ")")
    s"AggregatedRelation[id=${getRelationId()}][source=$tableName][cols=$columns][group=$groups][func=$func]"
  }

  var nodeSet: Set[Variable] = variables
}
case class TableScanRelation(val tableName: String, variables: Set[Variable]) extends Relation {

  def getTableName(): String = tableName

  override def toString: String = {
    val columns = nodeSet.map(n => n.name + ":" + n.dataType).mkString("(", ",", ")")
    s"TableScanRelation[id=${getRelationId()}][source=$tableName][cols=$columns]"
  }

  var nodeSet: Set[Variable] = variables
}
