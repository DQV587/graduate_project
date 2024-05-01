package graduateProject.planner.entity.physicalPlanVariable

import graduateProject.planner.entity.data_type.DataType

trait VariableInformation{
  val name:String
  def getVariableName:String=name
}

case class ArrayTypeVariable(name:String,columns:Array[(String,DataType)]) extends VariableInformation{
  override def toString: String = {
    name+" : "+columns.mkString("(",",",")")
  }
}

case class KeyArrayTypeVariable(name:String,keyIndex:Int,columns:Array[(String,DataType)]) extends VariableInformation{
  override def toString: String = {
    name + " : " +"key:"+keyIndex+ columns.mkString("(", ",", ")")
  }
}

case class KeyGroupByTypeVariable(name:String) extends VariableInformation

case class KeyGroupByArrayTypeVariable(name:String) extends VariableInformation

case class KeyValueTypeVariable(name:String,key:(String,DataType),value:(String,DataType)) extends VariableInformation
