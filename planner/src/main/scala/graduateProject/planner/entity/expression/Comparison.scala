package graduateProject.planner.entity.expression

import graduateProject.planner.entity.data_type.DataType

case class Comparison(op:ComparisonOperator,l:Expression,r:Expression) {
  val operator:ComparisonOperator=op
  val left:Expression=l
  val right:Expression=r
  val data_type:DataType=DataType.typeCast(left.getType,right.getType)

  override def toString: String = left.toString+" "+operator.toString+" "+right.toString
}
