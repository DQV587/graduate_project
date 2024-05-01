package graduateProject.planner.entity.expression

import graduateProject.planner.entity.data_type.DataType

case class Comparison(operator:ComparisonOperator,left:Expression,right:Expression) {
  val data_type:DataType=DataType.typeCast(left.getType,right.getType)
  override def toString: String = left.toString+" "+operator.toString+" "+right.toString
}
