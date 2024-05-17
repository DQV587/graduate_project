package graduateProject.planner.codeGenerator

import graduateProject.planner.entity.dataType.DataType
import graduateProject.planner.planGenerator.{AfterAction, AggregatedTableArrayByKeyAction, AppendKey2TupleAction, AppendKeyValueAction, BeforeAction, CalculateAggregateTableAction, CompleteAction, CountResultAction, CqcAction, CreateComparisonFunctionAction, CreateSortComparisonFunctionAction, CreateTableFromFileAction, EnumerateWithNoComparisonAction, EnumerateWithOneComparisonAction, EnumerateWithTwoComparisonsAction, GetMfFromOneDimArrayAction, GetMfFromSortedGroupByKeyAction, KeyArrayGroupByKeyAction, NoIncidentComparisonsReduce, PersistResultAction, PhysicalPlan, ReKeyAction, SelfFilterAction, SortByOneDimArrayAction, SortGroupByKeyAction, SourceTableArrayByKeyAction}

import scala.collection.mutable

object GenerateCode {
  def indent(builder: mutable.StringBuilder, n: Int): mutable.StringBuilder = {
    assert(n >= 0)
    for (_ <- 0 until n)
      builder.append("\t")
    builder
  }

  def newLine(builder: mutable.StringBuilder, n: Int = 1): mutable.StringBuilder = {
    assert(n >= 0)
    for (_ <- 0 until n)
      builder.append("\n")
    builder
  }

  def getImports: List[String] = List(
    "org.apache.spark.sql.SparkSession",
    "org.apache.spark.SparkConf",
    "org.slf4j.LoggerFactory",
    "graduateProject.basicLib.CqcConversions._"
  )
  def getType: String = "object"
  def getLogger: String = "SparkCqcQuery"
  def getName: String = "QueryProcess"
  def getAppName:String="CqcQueryProcess"
  private def generateSparkInit(builder: mutable.StringBuilder): Unit = {
    indent(builder, 2).append("val conf = new SparkConf()").append("\r\n")
    indent(builder, 2).append("conf.setAppName(\"").append(getAppName).append("\")\r\n")

    indent(builder, 2).append("val spark = SparkSession.builder.config(conf).getOrCreate()").append("\r\n")
  }

  private def generateSparkClose(builder: mutable.StringBuilder): Unit = {
    newLine(builder)
    indent(builder, 2).append("spark.close()").append("\r\n")
  }

  private def generateReadSourceTable(builder: mutable.StringBuilder, action: CreateTableFromFileAction): Unit = {
    indent(builder, 2).append("val ").append(action.variableName).append(" = spark.sparkContext.textFile(\"")
      .append(action.filePath).append("\").map(row => {").append("\r\n")
    indent(builder, 3).append("val f = row.split(\"").append(action.delimiter).append("\")").append("\r\n")

    val fields = {for(i<-action.columns.indices)
    yield DataType.fromTypeName(action.columns(i).getType).fromString(s"f(${i})")}

    indent(builder, 3).append("Array[Any](").append(fields.mkString(", ")).append(")").append("\r\n")
    indent(builder, 2).append("}).persist()").append("\r\n")
    indent(builder, 2).append(action.variableName).append(".count()").append("\r\n")
  }

  private def generateComputeAggregatedRelationAction(builder: mutable.StringBuilder, action: CalculateAggregateTableAction): Unit = {
    action.aggregateFunc match {
      case "COUNT" =>
        val groupFieldsString = s"f(${action.groupByIndex})"
        indent(builder, 2).append("val ").append(action.variableName).append(" = ").append(action.sourceVariable)
          .append(".map(f => (").append(groupFieldsString).append(", 1L))")
          .append(".reduceByKey(_ + _)")
          .append(".map(x => Array[Any](" + (0 to 1).map(i => "x._" + (i + 1)).mkString(", ") + ")).persist()").append("\r\n")
        indent(builder, 2).append(action.variableName).append(".count()").append("\r\n")
      case _ => throw new UnsupportedOperationException
    }
  }

  private def generateComparisonFunctionDefinition(builder: mutable.StringBuilder, action: CreateComparisonFunctionAction): Unit = {
    indent(builder,2).append("val ")
      .append(s"comparisonFunc${action.index} = (${action.leftExpr.getVariables.head.name}: ")
      .append(action.dataType.getScalaTypeName)
      .append(s", ${action.rightExpr.getVariables.head.name}: ")
      .append(action.dataType.getScalaTypeName).append(") => ")
      .append(action.leftExpr.toString).append(" ")
      .append(action.operator.OpeString).append(" ")
      .append(action.rightExpr.toString)
    newLine(builder)
  }
  private def generateSortFunctionDefinition(builder: mutable.StringBuilder,action:CreateSortComparisonFunctionAction):Unit={
    indent(builder,2).append("val ")
      .append(s"sortFunc${action.index} = (x: ")
      .append(action.dataType.getScalaTypeName)
      .append(", y: ").append(action.dataType.getScalaTypeName)
      .append(") => x ").append(action.operator.toString).append(" y")
    newLine(builder)
  }
  private def generateBeforeAction(beforeActions:List[BeforeAction], builder: mutable.StringBuilder):Unit={
    beforeActions.foreach {
      case createTableFromFileAction: CreateTableFromFileAction => generateReadSourceTable(builder,createTableFromFileAction)
      case calculateAggregateTableAction:CalculateAggregateTableAction=>generateComputeAggregatedRelationAction(builder,calculateAggregateTableAction)
      case createComparisonFunctionAction: CreateComparisonFunctionAction=>generateComparisonFunctionDefinition(builder,createComparisonFunctionAction)
      case createSortComparisonFunctionAction: CreateSortComparisonFunctionAction=>generateSortFunctionDefinition(builder,createSortComparisonFunctionAction)
    }
  }
  private def generateArrayByKeyAction(builder: mutable.StringBuilder,action:SourceTableArrayByKeyAction):Unit={
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.oldName)
      .append(s".keyBy(x => ${action.key._2.castFromAny(s"x(${action.key._1})")})")
      .append("\r\n")
  }
  private def generateAggregatedTableArrayByKeyAction(builder: mutable.StringBuilder,action:AggregatedTableArrayByKeyAction):Unit={
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.oldName).append(s".map(x => (${action.dataType.castFromAny(s"x(${action.keyIndex})")}, x(${action.valueIndex})))").append("\r\n")
  }
  private def generateReKeyAction(builder: mutable.StringBuilder,action:ReKeyAction):Unit={
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.oldName).append(".reKeyBy(x => ")
      .append(action.key._2.castFromAny(s"x(${action.key._1})"))
      .append(")").append("\r\n")
  }
  private def generateSemiJoinAction(builder: mutable.StringBuilder,action:NoIncidentComparisonsReduce):Unit={
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.oldName).append(".semiJoin(")
      .append(action.otherName).append(")").append("\r\n")
  }
  private def generateAppendCommonExtraColumnAction(builder: mutable.StringBuilder, action: AppendKeyValueAction): Unit = {
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.appendTo).append(".appendMf(")
      .append(action.appendFrom).append(")").append("\r\n")
  }
  private def generateGetValueFuncAction(index:Int,dataType: DataType):String={
    s"(array:Array[Any])=>${dataType.castFromAny(s"array(${index})")}"
  }
  private def generateCompareFuncParameters(isLeft:Boolean):String={
    if(isLeft) "(x,y)"
    else "(y, x)"
  }
  private def generateComparisonFuncName(index:Int):String={
    s"comparisonFunc${index}"
  }
  private def generateSortFuncName(index:Int):String={
    s"sortFunc${index}"
  }
  private def generateParameterDeclaration(dataType: DataType):String={
    s"(x: ${dataType.getScalaTypeName}, y: ${dataType.getScalaTypeName}) => "
  }
  private def generateAppendComparisonExtraColumnAction(builder: mutable.StringBuilder, action: AppendKey2TupleAction): Unit = {
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.appendTo).append(".appendMf")
      .append(s"[${action.dataType1.getScalaTypeName}, ${action.dataType2.getScalaTypeName}](")
      .append(action.appendFrom).append(", ")
      .append(generateGetValueFuncAction(action.compareValueIndex,action.dataType1)).append(", ")
      .append(generateParameterDeclaration(action.dataType1))
      .append(generateComparisonFuncName(action.comparisonIndex))
      .append(generateCompareFuncParameters(action.isLeft)).append(")").append("\r\n")
  }

  private def generateApplySelfComparisonAction(builder: mutable.StringBuilder, action:SelfFilterAction): Unit = {
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.oldName).append(".filter(")
      .append("x => ").append(generateComparisonFuncName(action.comparisonIndex)).append("(")
      .append(action.dataType.castFromAny(s"x._2(${action.leftIndex})")).append(", ")
      .append(action.dataType.castFromAny(s"x._2(${action.rightIndex})")).append(")")
      .append(")").append("\r\n")
  }
  private def generateGroupByAction(builder: mutable.StringBuilder, action: KeyArrayGroupByKeyAction): Unit = {
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.oldName).append(".groupBy()").append("\r\n")
  }
  private def generateSortGroupByKeyAction(builder: mutable.StringBuilder,action:SortGroupByKeyAction):Unit={
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.oldName).append(s".sortWith[${action.value._2.getScalaTypeName}](")
      .append(generateGetValueFuncAction(action.value._1,action.value._2)).append(", ")
      .append(generateParameterDeclaration(action.value._2))
      .append(generateSortFuncName(action.comparisonIndex))
      .append(generateCompareFuncParameters(action.isLeft))
      .append(")").append("\r\n")

  }
  private def generateGetMfFromSortedGroupAction(builder: mutable.StringBuilder,action:GetMfFromSortedGroupByKeyAction):Unit={
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.oldName).append(".getMf(")
      .append(generateGetValueFuncAction(action.value._1,action.value._2))
      .append(")").append("\r\n")
  }
  private def generateSortByOneDimArrayAction(builder: mutable.StringBuilder,action:SortByOneDimArrayAction):Unit={
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.oldName).append(s".sortByOneDimArray[")
      .append(action.value1._2.getScalaTypeName).append(", ")
      .append(action.value2._2.getScalaTypeName).append("](")
      .append(generateGetValueFuncAction(action.value1._1,action.value1._2)).append(", ")
      .append(generateGetValueFuncAction(action.value2._1,action.value2._2)).append(", ")
      .append(generateParameterDeclaration(action.value1._2))
      .append(generateSortFuncName(action.comparisonIndex1))
      .append(generateCompareFuncParameters(action.isLeft1))
      .append(", ")
      .append(generateParameterDeclaration(action.value2._2))
      .append(generateSortFuncName(action.comparisonIndex2))
      .append(generateCompareFuncParameters(action.isLeft2))
      .append(")").append("\r\n")
  }
  private def generateGetMfFromOneDimArrayAction(builder: mutable.StringBuilder,action:GetMfFromOneDimArrayAction):Unit={
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.oldName).append(".getMf").append("\r\n")
  }

  private def generateEnumerateWithoutComparisonAction(builder: mutable.StringBuilder, action: EnumerateWithNoComparisonAction): Unit = {
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.oldName).append(".enumerateWithNoComparison[")
      .append(action.keyType.getScalaTypeName).append("](")
      .append(action.otherName).append(", ")
      .append(action.thisIndices.mkString("Array(", ",", ")")).append(", ")
      .append(action.otherIndices.mkString("Array(", ",", ")"))
//    if (action.resultKeySelectors.nonEmpty) {
//      builder.append(", ").append(action.resultKeySelectors.map(s => s("l", "r")).mkString("(l, r) => (", ",", ")"))
//    }
    builder.append(")").append("\r\n")
  }

  private def generateEnumerateWithOneComparisonAction(builder: mutable.StringBuilder, action: EnumerateWithOneComparisonAction): Unit = {
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.oldName).append(s".enumerateWithOneComparison[${action.keyType.getScalaTypeName},${action.comparisonType.getScalaTypeName}](")
      .append(action.otherName).append(", ")
      .append(generateGetValueFuncAction(action.valueIndex1,action.comparisonType)).append(", ")
      .append(generateGetValueFuncAction(action.valueIndex2,action.comparisonType)).append(", ")
      .append(generateParameterDeclaration(action.comparisonType))
      .append(generateComparisonFuncName(action.comparisonIndex))
      .append(generateCompareFuncParameters(action.isLeft)).append(", ")
      .append(action.thisIndices.mkString("Array(", ",", ")")).append(", ")
      .append(action.otherIndices.mkString("Array(", ",", ")"))

//    if (action.resultKeySelectors.nonEmpty) {
//      builder.append(", ").append(action.resultKeySelectors.map(s => s("l", "r")).mkString("(l, r) => (", ",", ")"))
//    }
    builder.append(")").append("\r\n")
  }

  private def generateEnumerateWithTwoComparisonsAction(builder: mutable.StringBuilder, action: EnumerateWithTwoComparisonsAction): Unit = {
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.oldName).append(s".enumerateWithTwoComparisons[${action.keyType.getScalaTypeName},${action.valueType1.getScalaTypeName},${action.valueType2.getScalaTypeName}](")
      .append(action.otherName).append(", ")
      .append(generateGetValueFuncAction(action.valueIndex1, action.valueType1)).append(", ")
      .append(generateGetValueFuncAction(action.valueIndex2, action.valueType2)).append(", ")
      .append(action.thisIndices.mkString("Array(", ",", ")")).append(", ")
      .append(action.otherIndices.mkString("Array(", ",", ")"))

//    if (action.resultKeySelectors.nonEmpty) {
//      builder.append(", ").append(action.resultKeySelectors.map(s => s("l", "r")).mkString("(l, r) => (", ",", ")"))
//    }
    builder.append(")").append("\r\n")
  }
  private def generateCompleteAction(builder: mutable.StringBuilder, action:CompleteAction):Unit={
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.oldName).append("\r\n")
  }
  private def generateCqcAction(cqcActions: List[CqcAction],builder:mutable.StringBuilder): Unit = {
    cqcActions.foreach{
      case sourceTableArrayByKeyAction: SourceTableArrayByKeyAction =>generateArrayByKeyAction(builder,sourceTableArrayByKeyAction)
      case aggregatedTableArrayByKeyAction: AggregatedTableArrayByKeyAction=>generateAggregatedTableArrayByKeyAction(builder,aggregatedTableArrayByKeyAction)
      case reKeyAction: ReKeyAction=>generateReKeyAction(builder,reKeyAction)
      case noIncidentComparisonsReduce: NoIncidentComparisonsReduce=>generateSemiJoinAction(builder,noIncidentComparisonsReduce)
      case appendKeyValueAction: AppendKeyValueAction=>generateAppendCommonExtraColumnAction(builder,appendKeyValueAction)
      case appendKey2TupleAction: AppendKey2TupleAction=>generateAppendComparisonExtraColumnAction(builder,appendKey2TupleAction)
      case selfFilterAction: SelfFilterAction=>generateApplySelfComparisonAction(builder,selfFilterAction)
      case keyArrayGroupByKeyAction: KeyArrayGroupByKeyAction=>generateGroupByAction(builder,keyArrayGroupByKeyAction)
      case sortGroupByKeyAction: SortGroupByKeyAction=>generateSortGroupByKeyAction(builder,sortGroupByKeyAction)
      case getMfFromSortedGroupByKeyAction: GetMfFromSortedGroupByKeyAction=>generateGetMfFromSortedGroupAction(builder,getMfFromSortedGroupByKeyAction)
      case sortByOneDimArrayAction: SortByOneDimArrayAction=>generateSortByOneDimArrayAction(builder,sortByOneDimArrayAction)
      case getMfFromOneDimArrayAction: GetMfFromOneDimArrayAction=>generateGetMfFromOneDimArrayAction(builder,getMfFromOneDimArrayAction)
      case enumerateWithNoComparisonAction: EnumerateWithNoComparisonAction=>generateEnumerateWithoutComparisonAction(builder,enumerateWithNoComparisonAction)
      case enumerateWithOneComparisonAction: EnumerateWithOneComparisonAction=>generateEnumerateWithOneComparisonAction(builder,enumerateWithOneComparisonAction)
      case enumerateWithTwoComparisonsAction: EnumerateWithTwoComparisonsAction=>generateEnumerateWithTwoComparisonsAction(builder,enumerateWithTwoComparisonsAction)
      case completeAction: CompleteAction=>generateCompleteAction(builder,completeAction)
    }
  }

  def generateFormatResultAction(builder: mutable.StringBuilder, action: PersistResultAction): Unit = {
    val mapFunc = action.outputMap.map(i=>s"x._2($i)").mkString("x => Array(", ", ", ")")
    newLine(builder)
    indent(builder, 2).append("val ").append(action.newName).append(" = ")
      .append(action.resultName).append(".map(").append(mapFunc).append(")").append("\r\n")
    indent(builder, 2).append(action.newName).append(".take(20).map(r => r.mkString(\",\")).foreach(println)").append("\r\n")
    indent(builder, 2).append("println(\"only showing top 20 rows\")").append("\r\n")
  }

  def generateCountResultAction(builder: mutable.StringBuilder, action: CountResultAction): Unit = {
    if (getLogger.nonEmpty) {
      newLine(builder)
      indent(builder, 2).append("val ts1 = System.currentTimeMillis()").append("\r\n")
      indent(builder, 2).append("val cnt = ").append(action.resultName).append(".count()").append("\r\n")
      indent(builder, 2).append("val ts2 = System.currentTimeMillis()").append("\r\n")
      indent(builder, 2).append("LOGGER.info(\"").append(getName).append("-SparkCQC cnt: \" + cnt)").append("\r\n")
      indent(builder, 2).append("LOGGER.info(\"").append(getName).append("-SparkCQC time: \" + (ts2 - ts1) / 1000f)").append("\r\n")
    } else {
      newLine(builder)
      indent(builder, 2).append(action.resultName).append(".count()").append("\n")
    }
  }
  def generateAfterAction(afterActions:List[AfterAction],builder:mutable.StringBuilder): Unit = {
    afterActions.foreach {
      case countResultAction: CountResultAction=>generateCountResultAction(builder,countResultAction)
      case persistResultAction: PersistResultAction=>generateFormatResultAction(builder,persistResultAction)
    }
  }
  private def generateExecuteMethod(plan:PhysicalPlan, builder: mutable.StringBuilder): Unit = {
    indent(builder, 1).append("def main(args: Array[String]): Unit = {").append("\r\n")
    generateSparkInit(builder)

    generateBeforeAction(plan.beforeActions,builder)

    generateCqcAction(plan.cqcActions,builder)

    generateAfterAction(plan.afterActions,builder)

    generateSparkClose(builder)
    indent(builder, 1).append("}").append("\r\n")
  }
  private def generateBody(plan:PhysicalPlan,builder: mutable.StringBuilder): Unit = {
    if (getLogger.nonEmpty) {
      indent(builder, 1).append("val LOGGER = LoggerFactory.getLogger(\"").append(getLogger).append("\")\r\n")
      newLine(builder)
    }
    generateExecuteMethod(plan,builder)
  }

  private def generate(plan:PhysicalPlan,builder: mutable.StringBuilder): Unit = {
    val imports = getImports
    if (imports.nonEmpty) {
      imports.foreach(i => builder.append("import ").append(i).append("\r\n"))
      builder.append("\r\n")
    }

    builder.append(getType).append(" ").append(getName)
    builder.append(" {").append("\r\n")

    generateBody(plan,builder)

    builder.append("}").append("\r\n")
  }
  def apply(plan:PhysicalPlan,builder: mutable.StringBuilder):Unit={
    generate(plan,builder)
  }
}
