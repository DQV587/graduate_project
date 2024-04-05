package graduateProject.planner

import graduateProject.parser
import graduateProject.parser.CatalogManager
import graduateProject.parser.implLib.SQLParser
import graduateProject.parser.plan.SqlPlanner
import graduateProject.planner.algorithm.{GYO, RelNodeToQuery}
import graduateProject.planner.entity.hypergraph.relationHypergraph.RelationHyperGraph
import graduateProject.planner.entity.joinTree.JoinTree
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.{SqlNode, SqlNodeList}
object Main {
  def main(args: Array[String]) {
    val dml= "SELECT g1.src AS src, g1.dst AS via1, g3.src AS via2, g3.dst AS dst,\n" + "    c1.cnt AS cnt1, c2.cnt AS cnt2\n" + "FROM Graph AS g1, Graph AS g2, Graph AS g3,\n" + "    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c1,\n" + "    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c2\n" + "WHERE c1.src = g1.src AND g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = c2.src\n" + "    AND c1.cnt * 2 < c2.cnt"
    val tmp = SQLParser.parseDml(dml)
    val ddl = "CREATE TABLE Graph (\n" + "    src INT,\n" + "    dst INT\n" + ") WITH (\n" + "    'path' = 'examples/data/graph.dat'\n" + ")"
    val nodeList = SQLParser.parseDdl(ddl)
    val catalogManager = new CatalogManager
    catalogManager.register(nodeList)
    val crownPlanner = new SqlPlanner(catalogManager)
    val root = crownPlanner.toLogicalPlan(tmp)
    val query=RelNodeToQuery.convert(root)
    val hyperGraph=RelationHyperGraph.constructFromQuery(query)
    val joinTreeSet=GYO(hyperGraph)
  }

}