package graduateProject.planner.entity.dataType

import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName.{BIGINT, DOUBLE, INTEGER}

abstract class DataType {
  def getScalaTypeName: String
  def fromString(s: String): String

  def castFromAny(v: String): String =
    s"$v.asInstanceOf[$getScalaTypeName]"

  def format(x: String): String = s"$x.toString"
}

case object IntDataType extends DataType {
  override def fromString(s: String): String = s"$s.toInt"

  override def getScalaTypeName: String = "Int"
}

case object LongDataType extends DataType {
  override def fromString(s: String): String = s"$s.toLong"

  override def getScalaTypeName: String = "Long"
}


case object DoubleDataType extends DataType {
  override def getScalaTypeName: String = "Double"

  override def fromString(s: String): String = s"$s.toDouble"
}


object DataType {
  def fromSqlType(sqlTypeName: SqlTypeName): DataType = sqlTypeName match {
    case INTEGER => IntDataType
    case BIGINT => LongDataType
    case DOUBLE => DoubleDataType
    case _ => throw new UnsupportedOperationException(s"SqlType ${sqlTypeName.toString} is unsupported.")
  }

  def fromTypeName(typeName: String): DataType = typeName match {
    case "INTEGER" => IntDataType
    case "BIGINT" => LongDataType
    case "DOUBLE" => DoubleDataType
    case _ => throw new UnsupportedOperationException(s"SqlType ${typeName} is unsupported.")
  }

  def isNumericType(dataType: DataType): Boolean = dataType match {
    case IntDataType | LongDataType | DoubleDataType => true
    case _ => false
  }

  def typeCast(type1:DataType,type2:DataType):DataType={
    (type1, type2) match {
      case (DoubleDataType, LongDataType) => DoubleDataType
      case (DoubleDataType, IntDataType) => DoubleDataType
      case (LongDataType, DoubleDataType) => DoubleDataType
      case (IntDataType, DoubleDataType) => DoubleDataType
      case (IntDataType, LongDataType) => LongDataType
      case (LongDataType, IntDataType) => LongDataType
      case _ =>
        assert(type1 == type2)
        type1
    }
  }
}
