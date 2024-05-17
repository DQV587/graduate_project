package graduateProject.planner.innerRepresentation

import graduateProject.planner.entity.dataType._
import graduateProject.planner.entity.expression._
import graduateProject.planner.entity.hypergraph.relationHypergraph.{Relation, Variable}
import graduateProject.planner.entity.query.Query
import graduateProject.planner.entity.util.DisjointSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical._
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object RelNodeToQuery {

  def convert(root: RelNode): Query = {
    val logicalProject = root.asInstanceOf[LogicalProject]
    val logicalFilter = root.getInput(0).asInstanceOf[LogicalFilter]
    assert(logicalFilter.getCondition.isInstanceOf[RexCall])
    val condition = logicalFilter.getCondition.asInstanceOf[RexCall]
    assert(condition.getOperator.getName == "AND")
    val operands = condition.getOperands
    assert(operands.stream.allMatch((operand: RexNode) => operand.isInstanceOf[RexCall]))
    //get all variables and give a index
    val variableTableBuffer = new ArrayBuffer[Variable]()
    val disjointSet = new DisjointSet[Variable]()

    for (field <- logicalFilter.getInput.getRowType.getFieldList) {
      val fieldType = DataType.fromSqlType(field.getType.getSqlTypeName)
      val newVariable = Variable.newVariable(fieldType)
      disjointSet.makeNewSet(newVariable)
      variableTableBuffer.append(newVariable)
    }
    val variableTable = variableTableBuffer.toArray
    val conditions = ListBuffer.empty[RexCall]

    //get nature join and comparisons
    for (operand <- operands) {
      val rexCall: RexCall = operand.asInstanceOf[RexCall]
      rexCall.getOperator.getName match {
        case "=" =>
          val left: Int = extractIndexFromRexInputRef(rexCall.getOperands.get(0))
          val right: Int = extractIndexFromRexInputRef(rexCall.getOperands.get(1))

          val leftVariable = variableTable(left)
          val rightVariable = variableTable(right)
          disjointSet.merge(leftVariable, rightVariable)
        case "<" | "<=" | ">" | ">=" =>
          conditions.append(rexCall)
        case _ =>
          throw new UnsupportedOperationException(s"unsupported operator ${rexCall.getOperator.getName}")
      }
    }

    for (i <- variableTable.indices) {
      // replace variables in variableTable with their representatives
      variableTable(i) = disjointSet.getRepresentative(variableTable(i))
    }

    val relations = visitLogicalRelNode(logicalFilter.getInput, variableTable, 0)

    val comparisons = conditions.toList.map(rawComparison => {
      val op = Operator.getOperator(rawComparison.getOperator.getName).asInstanceOf[ComparisonOperator]
      val leftExpr = convertRexNodeToExpression(rawComparison.getOperands.get(0), variableTable)
      val rightExpr = convertRexNodeToExpression(rawComparison.getOperands.get(1), variableTable)
      Comparison(op,leftExpr,rightExpr)
    })

    val outputVariables = logicalProject.getProjects.toList.map(p => variableTable(p.asInstanceOf[RexInputRef].getIndex))
    val isFull = variableTable.forall(v => outputVariables.contains(v))

    Query(relations, comparisons, outputVariables, if (isFull) true else false)
  }

   private def extractIndexFromRexInputRef(rexNode: RexNode): Int = {
    rexNode match {
      case rexInputRef: RexInputRef => rexInputRef.getIndex
//      case call: RexCall if call.op.getName == "CAST" => extractIndexFromRexInputRef(call.getOperands.get(0))
      case _ => throw new UnsupportedOperationException(s"unsupported rexNode ${rexNode.toString}")
    }
  }

  private def visitLogicalRelNode(relNode: RelNode, variableTable: Array[Variable], offset: Int): List[Relation] = relNode match {
    case join: LogicalJoin => visitLogicalJoin(join, variableTable, offset)
    case aggregate: LogicalAggregate => visitLogicalAggregate(aggregate, variableTable, offset)
    case scan: LogicalTableScan => visitLogicalTableScan(scan, variableTable, offset)
    case _ => throw new UnsupportedOperationException
  }

  private def visitLogicalJoin(logicalJoin: LogicalJoin, variableTable: Array[Variable], offset: Int): List[Relation] = {
    val leftFieldCount = logicalJoin.getLeft.getRowType.getFieldCount
    visitLogicalRelNode(logicalJoin.getLeft, variableTable, offset) ++
      visitLogicalRelNode(logicalJoin.getRight, variableTable, offset + leftFieldCount)
  }

  private def visitLogicalAggregate(logicalAggregate: LogicalAggregate, variableTable: Array[Variable], offset: Int): List[Relation] = {
    // currently, only support patterns like '(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C1'
    assert(logicalAggregate.getGroupCount == 1)
    assert(logicalAggregate.getAggCallList.size() == 1)

    val group = logicalAggregate.getGroupSet.asList().map(_.toInt)
    val variables = variableTable.slice(offset, offset + logicalAggregate.getRowType.getFieldCount)
    val func = logicalAggregate.getAggCallList.get(0).getAggregation.toString

    assert(logicalAggregate.getInput.isInstanceOf[LogicalTableScan])
    val logicalTableScan = logicalAggregate.getInput.asInstanceOf[LogicalTableScan]
    val tableName = logicalTableScan.getTable.getQualifiedName.head

    List(Relation.newAggregateRelation(tableName, variables.toSet, group.toList,func))
  }

  private def visitLogicalTableScan(logicalTableScan: LogicalTableScan, variableTable: Array[Variable], offset: Int): List[Relation] = {
    val tableName = logicalTableScan.getTable.getQualifiedName.head
    val variables = variableTable.slice(offset, offset + logicalTableScan.getRowType.getFieldCount)

    List(Relation.newTableScanRelation(tableName, variables.toSet))
  }

  def convertRexNodeToExpression(rexNode: RexNode, variableTable: Array[Variable]): Expression = {
    rexNode match {
      case rexInputRef: RexInputRef =>
        VariableExpression(variableTable(rexInputRef.getIndex))
      case rexCall: RexCall =>
        convertRexCallToExpression(rexCall, variableTable)
      case rexLiteral: RexLiteral =>
        convertRexLiteralToExpression(rexLiteral)
      case _ =>
        throw new UnsupportedOperationException(s"unknown rexNode type ${rexNode.getType}")
    }
  }

  def convertRexCallToExpression(rexCall: RexCall, variableTable: Array[Variable]): Expression = {
    val operator=Operator.getOperator(rexCall.op.getName)
    val left = convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable)
    val right = convertRexNodeToExpression(rexCall.getOperands.get(1), variableTable)
    val dataType=DataType.typeCast(left.getType,right.getType)
    BinaryExpression(operator = operator.asInstanceOf[CalculateOperator], left = left, right = right, dataType = dataType)
  }

  def convertRexLiteralToExpression(rexLiteral: RexLiteral): Expression = {
    rexLiteral.getType.getSqlTypeName.getName match {
      case "INTEGER" => IntLitExpression(rexLiteral.getValue.toString.toInt)
      case "DECIMAL" => DoubleLitExpression(rexLiteral.getValue.toString.toDouble)
      case "BIGINT"  => LongLitExpression(rexLiteral.getValue.toString.toLong)
      case _ => throw new UnsupportedOperationException("unsupported literal type " + rexLiteral.getType.getSqlTypeName.getName)
    }
  }
}
