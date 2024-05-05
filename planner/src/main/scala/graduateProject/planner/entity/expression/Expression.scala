package graduateProject.planner.entity.expression

import graduateProject.planner.entity.data_type._
import graduateProject.planner.entity.hypergraph.relationHypergraph.Variable

sealed trait Expression {
    def getType: DataType
    def getVariables: Set[Variable]
}

case class BinaryExpression(operator: CalculateOperator,left: Expression,right:Expression,dataType: DataType) extends Expression {


    def getComputeFunction: String ={
        s"(${left.toString} ${operator.toString} ${right.toString})"
    }
    def getOperator: String =this.operator.toString
    override def getType: DataType = this.dataType

    override def getVariables: Set[Variable] = this.left.getVariables++this.right.getVariables

    override def toString: String = this.getComputeFunction
}

sealed trait LiteralExpression[T] extends Expression {
    val value:T
    def getLiteralString: String = this.value.toString
    def getValue:T=this.value
    override def getVariables: Set[Variable] = Set()

    override def toString: String = this.getLiteralString
}

case class IntLitExpression(value:Int) extends LiteralExpression[Int]{
    override def getType: DataType = IntDataType
}

case class DoubleLitExpression(value:Double) extends LiteralExpression[Double]{
    override def getType: DataType = DoubleDataType
}

case class LongLitExpression(value:Long) extends LiteralExpression[Long]{
    override def getType: DataType = LongDataType
}

case class VariableExpression(variable: Variable) extends Expression {
    override def getType: DataType = variable.dataType

    override def getVariables: Set[Variable] = Set(variable)

    override def toString: String = variable.toString
}

object Expression{

}



