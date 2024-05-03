package graduateProject.planner.algorithm.planGenerator

import graduateProject.parser.CatalogManager
import graduateProject.parser.implLib.ddl.SqlTable
import graduateProject.planner.entity.data_type.{DataType, LongDataType}
import graduateProject.planner.entity.expression.Comparison
import graduateProject.planner.entity.hypergraph.comparisonHypergraph.{ComparisonHyperGraph, ReduceInformation}
import graduateProject.planner.entity.hypergraph.relationHypergraph.{AggregatedRelation, Relation, TableScanRelation, Variable}
import graduateProject.planner.entity.physicalPlanVariable._
import graduateProject.planner.entity.query.Query

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object GeneratePhysicalPlan {

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
  def getCqcActions(reduceInformationList:List[ReduceInformation],
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
            newName,0,1,thisRelation.func))
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
          val thisRelationVariableName=relationMapToVariable(thisRelation)
          val thisRelationVariableInformation=variableManager.get(thisRelationVariableName)
          //the last relation, reduce phase completed, begin enumeration phase
          if(reduceInformation.reducedJoinTreeEdge.isEmpty){
          }

          else{
            val reducedJoinTreeEdge=reduceInformation.reducedJoinTreeEdge.get
            val otherRelation=reducedJoinTreeEdge.getOtherRelation(thisRelation)
            val otherRelationVariableName=relationMapToVariable(otherRelation)
            val otherRelationVariableInformation=variableManager.get(otherRelationVariableName)
            val sharedVariable=reducedJoinTreeEdge.sharedVariable.head
            relationKeyedBy(thisRelation,sharedVariable,relationMapToVariable, variableManager, cqcActions)
            relationGroupByKey(thisRelation,relationMapToVariable, variableManager, cqcActions)
            relationKeyedBy(otherRelation,sharedVariable,relationMapToVariable, variableManager, cqcActions)
            reduceInformation.reduceComparisonInformation.size match {
              //no incident comparison, standard semiJoin
              //reduced relation remain origin key-group structure
              case 0=>{
                val newName=VariableManager.getNewVariableName
                cqcActions.append(NoIncidentComparisonsReduce(otherRelationVariableName,newName))
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
                    sortIndex,
                    comparisonMapToInt(comparisonInformation.comparison),
                    comparisonInformation.isLeft))
                variableManager.register(sortedName,thisRelationVariable.keyIndex,thisRelationVariable.columns,
                  sorted = true,sortIndex)
                relationMapToVariable(thisRelation)=sortedName

                // get Mf variable and append to other relation
                val mfName=VariableManager.getNewVariableName
                val thisRelationSortedVariable=variableManager.get(relationMapToVariable(thisRelation)).asInstanceOf[KeyGroupByTypeVariable]
                cqcActions.append(GetMfFromSortedGroupByKeyAction(thisRelationSortedVariable.name,mfName))
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
                  val comparison=reduceInformation.reduceComparisonInformation.head.comparison
                  val afterFilterName=VariableManager.getNewVariableName
                  val otherRelationVariableName=relationMapToVariable(otherRelation)
                  val otherRelationVariable=variableManager.get(otherRelationVariableName).asInstanceOf[KeyArrayTypeVariable]
                  val columns=otherRelationVariable.columns
                  val comparisonIndex=comparisonMapToInt(comparison)
                  val leftIndex=getVariableIndexFromArray(comparison.left.getVariables.head,columns)
                  val rightIndex=getVariableIndexFromArray(comparison.right.getVariables.head,columns)
                  cqcActions.append(SelfFilterAction(otherRelationVariableName,afterFilterName,comparisonIndex, leftIndex, rightIndex))
                  variableManager.register(afterFilterName,otherRelationVariable.keyIndex,otherRelationVariable.columns)
                  relationMapToVariable(otherRelation)=afterFilterName
                }
              }
              case 2=>{

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
  def apply(catalog: CatalogManager, query: Query, comparisonHyperGraph: ComparisonHyperGraph): Unit = {
    val relationMapToVariable=mutable.Map[Relation,String]()
    val comparisonMapToInt=mutable.Map[Comparison,Int]()
    val variableManager=new VariableManager()

    val beforeAction=getBeforeActions(catalog, query,
      relationMapToVariable, comparisonMapToInt, variableManager)

    val reduceInformationArray = mutable.ArrayBuffer[ReduceInformation]()
    val nodeNum = comparisonHyperGraph.nodeSet.size
    for (i <- 0 to nodeNum) {
      val reducibleRelationSet = comparisonHyperGraph.getReducibleRelations
      val information = comparisonHyperGraph.reduceRelation(reducibleRelationSet.head)
      reduceInformationArray.append(information)
    }
    val cqcAction=getCqcActions(reduceInformationArray.take(6).toList,relationMapToVariable,
      comparisonMapToInt, variableManager)

    println(cqcAction.mkString("\r\n"))
    println(variableManager)
    println(relationMapToVariable.mkString("\r\n"))
//    reduceInformationArray.foreach(x => println(x))
    //    PhysicalPlan()
  }
}
