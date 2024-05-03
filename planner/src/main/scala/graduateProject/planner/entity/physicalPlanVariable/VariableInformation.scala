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
    name + " : " +"keyIndex: "+keyIndex+" value: "+ columns.mkString("(", ",", ")")
  }
}

case class KeyGroupByTypeVariable(name:String,keyIndex:Int,columns:Array[(String,DataType)],sorted:Boolean,sortedByIndex:Int) extends VariableInformation{
  override def toString: String = {
    val sortedInformation=if(sorted) "sorted by index "+sortedByIndex else "not sorted"
    name + " : " + "grouped by keyIndex: " + keyIndex + " value: " + columns.mkString("(", ",", ")")+sortedInformation
  }
}

case class KeyOneDimArrayTypeVariable(name:String) extends VariableInformation

case class KeyValueTypeVariable(name:String,key:(String,DataType),value:(String,DataType)) extends VariableInformation{
  override def toString: String = {
    name+" : "+"(key: "+key+ " value: "+value+")"
  }
}
