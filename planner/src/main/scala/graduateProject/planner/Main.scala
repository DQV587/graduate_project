package graduateProject.planner

import graduateProject.parser.CatalogManager
import graduateProject.parser.implLib.SQLParser
import graduateProject.parser.plan.SqlPlanner
import graduateProject.planner.algorithm.innerRepresentation.{GYO, JoinTreeToComparisonHyperGraph, RelNodeToQuery}
import graduateProject.planner.algorithm.planGenerator.GeneratePhysicalPlan
import graduateProject.planner.codeGenerator.GenerateCode
import graduateProject.planner.entity.hypergraph.relationHypergraph.RelationHyperGraph

import java.io.{File, FileReader, PrintWriter}
import scala.collection.mutable
import scala.io.Source
object Main {
  def main(args: Array[String]) {
    val sqlPath="sql/sql2"
    val ddlPath=sqlPath+"/ddl.txt"
    val dmlPath=sqlPath+"/dml.txt"
    val ddlSource=Source.fromFile(ddlPath)
    val ddl = ddlSource.mkString
    ddlSource.close()
    val dmlSource = Source.fromFile(dmlPath)
    val dml= dmlSource.mkString
    dmlSource.close()
    val tmp = SQLParser.parseDml(dml)
    val nodeList = SQLParser.parseDdl(ddl)
    val catalogManager = new CatalogManager
    catalogManager.register(nodeList)
    val crownPlanner = new SqlPlanner(catalogManager)
    val root = crownPlanner.toLogicalPlan(tmp)
    val query=RelNodeToQuery.convert(root)
    val hyperGraph=RelationHyperGraph.constructFromQuery(query)
    assert(hyperGraph.isAcyclic)
    val joinTreeSet=GYO(hyperGraph)
//    println(joinTreeSet)
    val comparisonHyperGraphSet=joinTreeSet.map(joinTree=>JoinTreeToComparisonHyperGraph(joinTree,query.comparisons.toSet))
//    println(comparisonHyperGraphSet)
//    comparisonHyperGraphSet.foreach(chg=>println(chg.isBergeAcyclic))
    val acyclicCHG=comparisonHyperGraphSet.filter(chg=>chg.isBergeAcyclic)
    val physicalPlan=GeneratePhysicalPlan(catalogManager,query,acyclicCHG.head)
    val builder=new mutable.StringBuilder()
    GenerateCode(physicalPlan,builder)
    val write = new PrintWriter(new File("experiment/src/main/scala/QueryProcess.scala"))
    write.write(builder.toString())
    write.close()
  }
}