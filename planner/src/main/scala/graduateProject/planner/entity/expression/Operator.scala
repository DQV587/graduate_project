package graduateProject.planner.entity.expression

sealed trait Operator {
    def getOpeName: String
    def getOpeString: String
}

abstract class ComparisonOperator extends Operator{
    val OpeName:String
    val OpeString:String

    final override def getOpeName: String = this.OpeName

    final override def getOpeString: String = this.OpeString

    override def toString:String = this.OpeString
}

case object LessThanOperator extends ComparisonOperator{
    override val OpeName: String = "LessThan"
    override val OpeString: String = "<"
}

case object LessOrEqualToOperator extends ComparisonOperator{
    override val OpeName: String = "LessOrEqualTo"
    override val OpeString: String = "<="
}

case object GreaterThanOperator extends ComparisonOperator{
    override val OpeName: String = "GreaterThan"
    override val OpeString: String = ">"
}

case object GreaterOrEqualToOperator extends ComparisonOperator{
    override val OpeName: String = "GreaterOrEqualTo"
    override val OpeString: String = ">="
}

abstract class CalculateOperator extends Operator{
    val OpeName: String
    val OpeString: String

    final override def getOpeName: String = this.OpeName

    final override def getOpeString: String = this.OpeString

    override def toString:String = this.OpeString
}

case object PlusOperator extends CalculateOperator{
    override val OpeName: String = "Plus"
    override val OpeString: String = "+"
}

case object TimeOperator extends CalculateOperator{
    override val OpeName: String = "Time"
    override val OpeString: String = "*"
}

object Operator {
    def getOperator(op: String): Operator = {
        op match {
            case "<"  => LessThanOperator
            case "<=" => LessOrEqualToOperator
            case ">"  => GreaterThanOperator
            case ">=" => GreaterOrEqualToOperator
            case "+"  => PlusOperator
            case "*"  => TimeOperator
            case _ => throw new UnsupportedOperationException(s"Operator $op is not supported yet")
        }
    }
}


