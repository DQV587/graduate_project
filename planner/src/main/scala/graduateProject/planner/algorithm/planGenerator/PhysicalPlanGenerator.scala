package graduateProject.planner.algorithm.planGenerator

import graduateProject.parser.CatalogManager
import graduateProject.parser.implLib.ddl.SqlTable
import graduateProject.planner.entity.dataType.{DataType, LongDataType}
import graduateProject.planner.entity.expression.Comparison
import graduateProject.planner.entity.hypergraph.comparisonHypergraph.{ComparisonHyperGraph, ReduceComparisonInformation, ReduceInformation}
import graduateProject.planner.entity.hypergraph.relationHypergraph.{AggregatedRelation, Relation, TableScanRelation, Variable}
import graduateProject.planner.entity.physicalPlanVariable._
import graduateProject.planner.entity.query.Query

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import java.io.{File, PrintWriter}

object PhysicalPlanGenerator {

  def getSourceTableNames(relations: Set[Relation]):Set[String]={
    relations.map(relation=>relation.getTableName())
  }
  def getAggregateRelations(relations: Set[Relation]):Set[AggregatedRelation]={
    relations.filter(relation=>relation.isInstanceOf[AggregatedRelation])
      .map(relation=>relation.asInstanceOf[AggregatedRelation])
  }
  def getTuple3FromAggregatedRelation(relation: AggregatedRelation):(String,Int,String)={
    (relation.getTableName(),relation.group.head,relation.func)
  }
  def getVariableIndexFromRelation(key:Variable, relation: Relation):Int ={
    var i = 0
    var index = 0
    for (variable <- relation.getVariables()) {
      if (!variable.name.equals(key.name)) {
        i += 1
      }
      else {
        index = i
      }
    }
    index
  }
  def getVariableIndexFromArray(key:Variable, columns:Array[(String,DataType)]):Int={
    var i=0
    var index=0
    for(variable<-columns){
      if(!variable._1.equals(key.name)){
        i+=1
      }
      else{
        index=i
      }
    }
    index
  }
  def arrayToKeyArrayType(key:Variable, relation: Relation,
                          relationMapToVariable:mutable.Map[Relation, String],
                          variableManager:VariableManager,
                          cqcActions: mutable.ListBuffer[CqcAction]):Unit={
    val columnArray = mutable.ArrayBuffer[(String, DataType)]()
    val index = getVariableIndexFromRelation(key, relation)
    for (variable <- relation.getVariables()) {
      columnArray.append((variable.name, variable.dataType))
    }
    val newName = VariableManager.getNewVariableName
    val columns = columnArray.toArray
    cqcActions.append(
      SourceTableArrayByKeyAction(relationMapToVariable(relation), newName,
        (index, columns(index)._2))
    )
    relationMapToVariable(relation) = newName
    variableManager.register(newName, index, columns)
  }
  def reKeyArray(key:Variable, columns:Array[(String,DataType)],
                 relation: Relation,
                 relationMapToVariable: mutable.Map[Relation, String],
                 variableManager: VariableManager,
                 cqcActions: mutable.ListBuffer[CqcAction]):Unit={
    val newName = VariableManager.getNewVariableName
    val index = getVariableIndexFromArray(key, columns)
    cqcActions.append(ReKeyAction(relationMapToVariable(relation), newName, (index, columns(index)._2)))
    relationMapToVariable(relation) = newName
    variableManager.register(newName, index, columns)
  }
  def relationKeyedBy(relation: Relation, key:Variable,
                      relationMapToVariable: mutable.Map[Relation, String],
                      variableManager: VariableManager,
                      cqcActions: mutable.ListBuffer[CqcAction]):Unit={
    variableManager.get(relationMapToVariable(relation)) match {
      case arrayType: ArrayTypeVariable => {
        arrayToKeyArrayType(key, relation,
          relationMapToVariable, variableManager, cqcActions)
      }
      case keyArrayType: KeyArrayTypeVariable =>
        // has different key for the two variable, so the other relation need to perform reKey action
        if (!keyArrayType.columns(keyArrayType.keyIndex)._1.equals(key.name)) {
          reKeyArray(key, keyArrayType.columns, relation, relationMapToVariable, variableManager, cqcActions)
        }
    }
  }
  def relationGroupByKey(relation: Relation,
                         relationMapToVariable: mutable.Map[Relation, String],
                         variableManager: VariableManager,
                         cqcActions: mutable.ListBuffer[CqcAction]):Unit={
    val oldName=relationMapToVariable(relation)
    val variableInformation=variableManager.get(oldName)
    assert(variableInformation.isInstanceOf[KeyArrayTypeVariable])
    val newName=VariableManager.getNewVariableName
    cqcActions.append(KeyArrayGroupByKeyAction(oldName, newName))
    variableManager.register(newName,variableInformation.asInstanceOf[KeyArrayTypeVariable].keyIndex,
      variableInformation.asInstanceOf[KeyArrayTypeVariable].columns,sorted = false)
    relationMapToVariable(relation)=newName
  }
  def filterInKeyArray(relation: Relation,comparisonInformation: ReduceComparisonInformation,
                       relationMapToVariable: mutable.Map[Relation, String],
                       variableManager: VariableManager,
                       comparisonMapToInt: mutable.Map[Comparison, Int],
                       cqcActions: mutable.ListBuffer[CqcAction]):Unit={
    val comparison = comparisonInformation.comparison
    val afterFilterName = VariableManager.getNewVariableName
    val otherRelationVariableName = relationMapToVariable(relation)
    val otherRelationVariable = variableManager.get(otherRelationVariableName).asInstanceOf[KeyArrayTypeVariable]
    val columns = otherRelationVariable.columns
    val comparisonIndex = comparisonMapToInt(comparison)
    val leftIndex = getVariableIndexFromArray(comparison.left.getVariables.head, columns)
    val rightIndex = getVariableIndexFromArray(comparison.right.getVariables.head, columns)
    cqcActions.append(SelfFilterAction(otherRelationVariableName, afterFilterName, comparisonIndex, leftIndex, rightIndex,comparison.data_type))
    variableManager.register(afterFilterName, otherRelationVariable.keyIndex, otherRelationVariable.columns)
    relationMapToVariable(relation) = afterFilterName
  }
  def getBeforeActions(catalog: CatalogManager, query: Query,
    relationMapToVariable:mutable.Map[Relation, String],
    comparisonMapToInt:mutable.Map[Comparison, Int],
    variableManager:VariableManager):List[BeforeAction]={
    val relations = query.relations
    val sourceTableNames = getSourceTableNames(relations.toSet)
    val tables = sourceTableNames.map(tableName => catalog.getSchema.getTable(tableName, false).getTable).map(t => t.asInstanceOf[SqlTable])
    val beforeActionList = mutable.ListBuffer[BeforeAction]()
    //read file and create table view  AND register the table variable name
    tables.foreach(table => {
      beforeActionList.append(
        CreateTableFromFileAction(table.getTableName,
          table.getTableColumns,
          table.getTableProperties.get("path"),
          table.getTableProperties.get("delimiter")))
      val columnsInformation = mutable.ArrayBuffer[(String, DataType)]()
      table.getTableColumns.foreach(x =>
        columnsInformation.append((x.getName, DataType.fromTypeName(x.getType))))
      variableManager.register(table.getTableName, columnsInformation.toArray)
    })
    relations.filter(relation => relation.isInstanceOf[TableScanRelation])
      .foreach(relation => relationMapToVariable(relation) = relation.getTableName())

    // create table from exist table by aggregate function
    val aggregatedRelations = getAggregateRelations(relations.toSet)
    val aggregatedTableSet = aggregatedRelations.map(relation => getTuple3FromAggregatedRelation(relation))
    val aggregatedTableToVariableMap = aggregatedTableSet.map(tuple3 =>
      (tuple3, tuple3._1 + "AggregatedWithValue" + tuple3._2.toString + "By" + tuple3._3)).toMap
    aggregatedTableSet.foreach(table => {
      // generate action
      beforeActionList.append(
        CalculateAggregateTableAction(aggregatedTableToVariableMap(table),
          table._1, table._2, table._3)
      )
      // register variable and bind the relation to that
      val columnInformation = ArrayBuffer[(String, DataType)]()
      columnInformation.append(
        (variableManager.get(table._1).asInstanceOf[ArrayTypeVariable].columns(table._2)._1,
          variableManager.get(table._1).asInstanceOf[ArrayTypeVariable].columns(table._2)._2))
      columnInformation.append(("aggregateFuncValue", LongDataType))
      variableManager.register(aggregatedTableToVariableMap(table), columnInformation.toArray)
    })
    aggregatedRelations.foreach(relation => {
      val tuple3 = getTuple3FromAggregatedRelation(relation)
      relationMapToVariable(relation) = aggregatedTableToVariableMap(tuple3)
    })

    // parse comparison
    val comparisons = query.comparisons
    var comparisonIndex = 0
    comparisons.foreach(comparison => {
      comparisonIndex += 1
      comparisonMapToInt(comparison) = comparisonIndex
      beforeActionList.append(
        CreateComparisonFunctionAction(comparisonMapToInt(comparison),
          comparison.operator,
          comparison.left, comparison.right,
          comparison.data_type)
      )
      beforeActionList.append(
        CreateSortComparisonFunctionAction(comparisonMapToInt(comparison),
          comparison.operator,
          comparison.data_type)
      )
    })
    beforeActionList.toList
  }
  def getOutputIndices(columns:Array[(String,DataType)],output:Set[String]):List[Int]={
    columns.indices.filter(i=>output.contains(columns(i)._1)).toList
  }


  def getCqcActions(query: Query,
                    reduceInformationList:List[ReduceInformation],
                    relationMapToVariable: mutable.Map[Relation, String],
                    comparisonMapToInt: mutable.Map[Comparison, Int],
                    variableManager: VariableManager ):List[CqcAction]={
    val cqcActions=mutable.ListBuffer[CqcAction]()
    val enumeratePhaseStack=mutable.Stack[ReduceInformation]()
    reduceInformationList.foreach(reduceInformation=>{
      reduceInformation.relation match {
        case aggregatedRelation: AggregatedRelation=>{
          val thisRelation=aggregatedRelation
          val thisRelationVariable=variableManager.get(relationMapToVariable(thisRelation))
            .asInstanceOf[ArrayTypeVariable]
          val newName=VariableManager.getNewVariableName
          cqcActions.append(AggregatedTableArrayByKeyAction(thisRelationVariable.name,
            newName,0,1,thisRelationVariable.columns(0)._2))
          relationMapToVariable(thisRelation)=newName
          val joinTreeEdge=reduceInformation.reducedJoinTreeEdge.get
          val sharedVariable=joinTreeEdge.sharedVariable.head
          val valueVariable=(thisRelation.nodeSet-sharedVariable).head
          variableManager.register(newName,
            (sharedVariable.name,sharedVariable.dataType),(valueVariable.name,valueVariable.dataType))
          val otherRelation=joinTreeEdge.getOtherRelation(thisRelation)
          relationKeyedBy(otherRelation,sharedVariable,relationMapToVariable, variableManager, cqcActions)
          val afterAppendName=VariableManager.getNewVariableName
          cqcActions.append(AppendKeyValueAction(relationMapToVariable(otherRelation),
            relationMapToVariable(thisRelation),afterAppendName))
          val otherRelationVariable1=variableManager.get(relationMapToVariable(otherRelation)).asInstanceOf[KeyArrayTypeVariable]
          variableManager.register(afterAppendName,otherRelationVariable1.keyIndex,
            otherRelationVariable1.columns:+(valueVariable.name,valueVariable.dataType))
          relationMapToVariable(otherRelation)=afterAppendName
      }
        case tableScanRelation: TableScanRelation => {
          val thisRelation=tableScanRelation
          //the last relation, reduce phase completed, begin enumeration phase
          if(reduceInformation.reducedJoinTreeEdge.isEmpty){
            val outputColumns=query.output.map(variable=>variable.name).toSet
            while(enumeratePhaseStack.nonEmpty){
              val curEnumerateInformation=enumeratePhaseStack.pop()
              val otherRelation = curEnumerateInformation.relation
              val sharedVariable = curEnumerateInformation.reducedJoinTreeEdge.get.sharedVariable.head
              relationKeyedBy(thisRelation, sharedVariable, relationMapToVariable, variableManager, cqcActions)
              val curEnumeratePhaseVariable=variableManager.get(relationMapToVariable(thisRelation)).asInstanceOf[KeyArrayTypeVariable]
              val thisIndices=getOutputIndices(curEnumeratePhaseVariable.columns,outputColumns)
              val newName=VariableManager.getNewVariableName
              curEnumerateInformation.reduceComparisonInformation.size match {
                case 0=>{
                  val otherRelationVariable=variableManager.get(relationMapToVariable(otherRelation)).asInstanceOf[KeyGroupByTypeVariable]
                  val otherIndices=getOutputIndices(otherRelationVariable.columns,
                    outputColumns--thisIndices.map(i=>curEnumeratePhaseVariable.columns(i)._1).toSet)
                  cqcActions.append(EnumerateWithNoComparisonAction(curEnumeratePhaseVariable.name,newName,otherRelationVariable.name,
                    thisIndices,otherIndices,curEnumeratePhaseVariable.columns(curEnumeratePhaseVariable.keyIndex)._2))
                  val newColumns=(thisIndices.map(i=>curEnumeratePhaseVariable.columns(i))++otherIndices.map(i=>otherRelationVariable.columns(i))).toArray
                  variableManager.register(newName,curEnumeratePhaseVariable.keyIndex,newColumns)
                  relationMapToVariable(thisRelation)=newName
                }
                case 1=>{
                  val reduceComparisonInformation=curEnumerateInformation.reduceComparisonInformation.head
                  val comparison=reduceComparisonInformation.comparison
                  val otherRelationVariable = variableManager.get(relationMapToVariable(otherRelation)).asInstanceOf[KeyGroupByTypeVariable]
                  assert(otherRelationVariable.sorted)
                  val otherIndices = getOutputIndices(otherRelationVariable.columns,
                    outputColumns -- thisIndices.map(i => curEnumeratePhaseVariable.columns(i)._1).toSet)
                  if(reduceComparisonInformation.isLeft)
                    cqcActions.append(EnumerateWithOneComparisonAction(curEnumeratePhaseVariable.name,newName,otherRelationVariable.name,
                      getVariableIndexFromArray(comparison.right.getVariables.head,curEnumeratePhaseVariable.columns),
                      getVariableIndexFromArray(comparison.left.getVariables.head,otherRelationVariable.columns),
                      comparisonMapToInt(comparison),isLeft = false,comparison.data_type,
                      thisIndices,otherIndices,curEnumeratePhaseVariable.columns(curEnumeratePhaseVariable.keyIndex)._2))
                  else
                    cqcActions.append(EnumerateWithOneComparisonAction(curEnumeratePhaseVariable.name, newName, otherRelationVariable.name,
                      getVariableIndexFromArray(comparison.left.getVariables.head, curEnumeratePhaseVariable.columns),
                      getVariableIndexFromArray(comparison.right.getVariables.head, otherRelationVariable.columns),
                      comparisonMapToInt(comparison), isLeft = true,comparison.data_type,
                      thisIndices, otherIndices, curEnumeratePhaseVariable.columns(curEnumeratePhaseVariable.keyIndex)._2))
                  val newColumns = (thisIndices.map(i => curEnumeratePhaseVariable.columns(i)) ++ otherIndices.map(i => otherRelationVariable.columns(i))).toArray
                  variableManager.register(newName, curEnumeratePhaseVariable.keyIndex, newColumns)
                  relationMapToVariable(thisRelation) = newName
                }
                case 2=>{
                  val comparisonInformationArray = curEnumerateInformation.reduceComparisonInformation.toArray
                  val index = {
                    if (comparisonInformationArray.head.isLong) 1
                    else 0
                  }
                  val indexComparisonInformation = comparisonInformationArray(index)
                  val valueComparisonInformation = comparisonInformationArray(1 - index)
                  val otherRelationVariable = variableManager.get(relationMapToVariable(otherRelation)).asInstanceOf[KeyOneDimArrayTypeVariable]
                  val otherIndices = getOutputIndices(otherRelationVariable.columns,
                    outputColumns -- thisIndices.map(i => curEnumeratePhaseVariable.columns(i)._1).toSet)
                  val valueIndex1=getVariableIndexFromArray({
                    if(indexComparisonInformation.isLeft)
                      indexComparisonInformation.comparison.right.getVariables.head
                    else
                      indexComparisonInformation.comparison.left.getVariables.head
                  }, curEnumeratePhaseVariable.columns)
                  val valueIndex2= getVariableIndexFromArray({
                    if (valueComparisonInformation.isLeft)
                      valueComparisonInformation.comparison.right.getVariables.head
                    else
                      valueComparisonInformation.comparison.left.getVariables.head
                  }, curEnumeratePhaseVariable.columns)
                  cqcActions.append(EnumerateWithTwoComparisonsAction(curEnumeratePhaseVariable.name,newName,otherRelationVariable.name,
                    valueIndex1,curEnumeratePhaseVariable.columns(valueIndex1)._2,
                    valueIndex2,curEnumeratePhaseVariable.columns(valueIndex2)._2,
                    thisIndices,otherIndices,
                    curEnumeratePhaseVariable.columns(curEnumeratePhaseVariable.keyIndex)._2))
                  val newColumns = (thisIndices.map(i => curEnumeratePhaseVariable.columns(i)) ++ otherIndices.map(i => otherRelationVariable.columns(i))).toArray
                  variableManager.register(newName, curEnumeratePhaseVariable.keyIndex, newColumns)
                  relationMapToVariable(thisRelation) = newName
                }
                case _=>{
                  //TODO
                }
              }

            }
            val resultName="result"
            val lastVariable=variableManager.get(relationMapToVariable(thisRelation)).asInstanceOf[KeyArrayTypeVariable]
            cqcActions.append(CompleteAction(lastVariable.name,resultName))
            variableManager.register(resultName,lastVariable.keyIndex,lastVariable.columns)
          }
          else{
            val reducedJoinTreeEdge=reduceInformation.reducedJoinTreeEdge.get
            val otherRelation=reducedJoinTreeEdge.getOtherRelation(thisRelation)
            val sharedVariable=reducedJoinTreeEdge.sharedVariable.head
            relationKeyedBy(thisRelation,sharedVariable,relationMapToVariable, variableManager, cqcActions)
            relationGroupByKey(thisRelation,relationMapToVariable, variableManager, cqcActions)
            relationKeyedBy(otherRelation,sharedVariable,relationMapToVariable, variableManager, cqcActions)
            reduceInformation.reduceComparisonInformation.size match {
              //no incident comparison, standard semiJoin
              //reduced relation remain origin key-group structure
              case 0=>{
                val newName=VariableManager.getNewVariableName
                val thisRelationName=relationMapToVariable(thisRelation)
                val otherRelationVariableName = relationMapToVariable(otherRelation)
                val otherRelationVariableInformation = variableManager.get(otherRelationVariableName)
                cqcActions.append(NoIncidentComparisonsReduce(otherRelationVariableName,newName,thisRelationName))
                variableManager.register(newName,otherRelationVariableInformation.asInstanceOf[KeyArrayTypeVariable].keyIndex,
                  otherRelationVariableInformation.asInstanceOf[KeyArrayTypeVariable].columns)
                relationMapToVariable(otherRelation)=newName
              }
              case 1=>{
                //get this relation sorted
                val comparisonInformation=reduceInformation.reduceComparisonInformation.head
                val oldName=relationMapToVariable(thisRelation)
                val sortedName=VariableManager.getNewVariableName
                val thisVariableInvolvedInComparison=if(comparisonInformation.isLeft)
                  comparisonInformation.comparison.left.getVariables.head
                else
                  comparisonInformation.comparison.right.getVariables.head
                val thisRelationVariable=variableManager.get(relationMapToVariable(thisRelation)).asInstanceOf[KeyGroupByTypeVariable]
                val sortIndex= getVariableIndexFromArray(thisVariableInvolvedInComparison, thisRelationVariable.columns)
                cqcActions.append(
                  SortGroupByKeyAction(
                    oldName,
                    sortedName,
                    (sortIndex,comparisonInformation.comparison.data_type),
                    comparisonMapToInt(comparisonInformation.comparison),
                    comparisonInformation.isLeft))
                variableManager.register(sortedName,thisRelationVariable.keyIndex,thisRelationVariable.columns,
                  sorted = true,sortIndex)
                relationMapToVariable(thisRelation)=sortedName

                // get Mf variable and append to other relation
                val mfName=VariableManager.getNewVariableName
                val thisRelationSortedVariable=variableManager.get(relationMapToVariable(thisRelation)).asInstanceOf[KeyGroupByTypeVariable]
                cqcActions.append(GetMfFromSortedGroupByKeyAction(thisRelationSortedVariable.name,mfName,
                  (sortIndex,thisRelationSortedVariable.columns(sortIndex)._2)))
                variableManager.register(mfName,
                  thisRelationSortedVariable.columns(thisRelationSortedVariable.keyIndex),
                  thisRelationSortedVariable.columns(thisRelationSortedVariable.sortedByIndex))
                val afterAppendName=VariableManager.getNewVariableName
                val otherRelationVariableName=relationMapToVariable(otherRelation)
                val otherRelationVariableInformation=variableManager.get(otherRelationVariableName).asInstanceOf[KeyArrayTypeVariable]
                cqcActions.append(AppendKeyValueAction(otherRelationVariableName,mfName,afterAppendName))
                variableManager.register(afterAppendName,
                  otherRelationVariableInformation.keyIndex,
                  otherRelationVariableInformation.columns:+variableManager.get(mfName).asInstanceOf[KeyValueTypeVariable].value)
                relationMapToVariable(otherRelation)=afterAppendName

                // check whether short comparison, if yes, then perform filter action
                if(!reduceInformation.reduceComparisonInformation.head.isLong){
                  filterInKeyArray(otherRelation,reduceInformation.reduceComparisonInformation.head,
                    relationMapToVariable,variableManager,comparisonMapToInt,cqcActions)
                }
              }
              case 2=>{
                // convert the key-group rdd to key-oneDimArray
                val comparisonInformationArray=reduceInformation.reduceComparisonInformation.toArray
                val index={
                  if(comparisonInformationArray.head.isLong) 1
                  else 0
                }
                val indexComparisonInformation=comparisonInformationArray(index)
                val valueComparisonInformation=comparisonInformationArray(1-index)
                val oneDimStructureName=VariableManager.getNewVariableName
                val oldName=relationMapToVariable(thisRelation)
                val thisRelationVariable=variableManager.get(oldName).asInstanceOf[KeyGroupByTypeVariable]
                val index1=getVariableIndexFromArray({
                  if(indexComparisonInformation.isLeft)
                    indexComparisonInformation.comparison.left.getVariables.head
                  else
                    indexComparisonInformation.comparison.right.getVariables.head},
                  thisRelationVariable.columns)
                val index2 = getVariableIndexFromArray({
                  if (valueComparisonInformation.isLeft)
                    valueComparisonInformation.comparison.left.getVariables.head
                  else
                    valueComparisonInformation.comparison.right.getVariables.head
                },
                  thisRelationVariable.columns)
                cqcActions.append(SortByOneDimArrayAction(oldName,oneDimStructureName,
                  (index1,thisRelationVariable.columns(index1)._2),comparisonMapToInt(indexComparisonInformation.comparison),indexComparisonInformation.isLeft,
                  (index2,thisRelationVariable.columns(index2)._2),comparisonMapToInt(valueComparisonInformation.comparison),valueComparisonInformation.isLeft
                ))
                variableManager.register(oneDimStructureName,thisRelationVariable.keyIndex,thisRelationVariable.columns,
                  index1, comparisonMapToInt(indexComparisonInformation.comparison), indexComparisonInformation.isLeft,
                  index2, comparisonMapToInt(valueComparisonInformation.comparison), valueComparisonInformation.isLeft)
                relationMapToVariable(thisRelation)=oneDimStructureName
                // get Mf column and append to the other relation
                val mfName=VariableManager.getNewVariableName
                val oneDimArrayVariable=variableManager.get(oneDimStructureName).asInstanceOf[KeyOneDimArrayTypeVariable]
                cqcActions.append(GetMfFromOneDimArrayAction(oneDimStructureName,mfName))
                val key=oneDimArrayVariable.columns(oneDimArrayVariable.keyIndex)
                val dim1=oneDimArrayVariable.columns(oneDimArrayVariable.valueIndex1)
                val dim2=oneDimArrayVariable.columns(oneDimArrayVariable.valueIndex2)
                variableManager.register(mfName,key,dim1,dim2)

                val otherRelationVariable=variableManager.get(relationMapToVariable(otherRelation)).asInstanceOf[KeyArrayTypeVariable]
                val compareValueIndex=getVariableIndexFromArray({
                  if(indexComparisonInformation.isLeft)
                    indexComparisonInformation.comparison.right.getVariables.head
                  else
                    indexComparisonInformation.comparison.left.getVariables.head
                },otherRelationVariable.columns)
                val newName=VariableManager.getNewVariableName
                val mfVariable=variableManager.get(mfName).asInstanceOf[KeyTwoValueTypeVariable]
                cqcActions.append(AppendKey2TupleAction(otherRelationVariable.name,mfName,
                  newName,compareValueIndex,comparisonMapToInt(indexComparisonInformation.comparison),
                  mfVariable.value1._2,mfVariable.value2._2,
                  indexComparisonInformation.isLeft))
                variableManager.register(newName,otherRelationVariable.keyIndex,otherRelationVariable.columns:+dim2)
                relationMapToVariable(otherRelation)=newName
                if(!valueComparisonInformation.isLong){
                  filterInKeyArray(otherRelation, valueComparisonInformation,
                    relationMapToVariable, variableManager, comparisonMapToInt, cqcActions)
                }
              }
              case _=>{
                //TODO
              }
            }
            enumeratePhaseStack.push(reduceInformation)
          }
        }
        case _=>throw new UnsupportedOperationException("not support this type Relation")
      }
    })
    cqcActions.toList
  }

  def getAfterActions(mode:String="count",outputMap:List[Int]):List[AfterAction] ={
    if(mode.equals("count"))
      List(CountResultAction("result"))
    else if(mode.equals("output"))
      List(PersistResultAction(VariableManager.getNewVariableName,"result",
        outputMap))
    else List()
  }
  def getOutputMap(variableManager:VariableManager,output:List[Variable]):List[Int]={
    val curColumns=variableManager.get("result").asInstanceOf[KeyArrayTypeVariable].columns
    val result=mutable.ListBuffer[Int]()
    output.foreach(variable=>result.append(getVariableIndexFromArray(variable,curColumns)))
    result.toList
  }
  def apply(catalog: CatalogManager, query: Query, reduceInformationList: List[ReduceInformation]): PhysicalPlan = {
    val relationMapToVariable=mutable.Map[Relation,String]()
    val comparisonMapToInt=mutable.Map[Comparison,Int]()
    val variableManager=new VariableManager()
    val beforeAction=getBeforeActions(catalog, query, relationMapToVariable, comparisonMapToInt, variableManager)
    val cqcAction=getCqcActions(query,reduceInformationList,relationMapToVariable,
      comparisonMapToInt, variableManager)
    val afterAction=getAfterActions("count",getOutputMap(variableManager,query.output))
    PhysicalPlan(beforeAction,cqcAction,afterAction)
  }
}