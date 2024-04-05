package graduateProject.parser;

import graduateProject.parser.CatalogManager;
import graduateProject.parser.implLib.SQLParser;
import graduateProject.parser.plan.SqlPlanner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;


public class Main {
    public static void main(String[] args) throws SqlParseException {
        String dml="SELECT g1.src AS src, g1.dst AS via1, g3.src AS via2, g3.dst AS dst,\n" +
                "    c1.cnt AS cnt1, c2.cnt AS cnt2\n" +
                "FROM Graph AS g1, Graph AS g2, Graph AS g3,\n" +
                "    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c1,\n" +
                "    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c2\n" +
                "WHERE c1.src = g1.src AND g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = c2.src\n" +
                "    AND c1.cnt * 2 < c2.cnt";
        SqlNode tmp= SQLParser.parseDml(dml);
        String ddl="CREATE TABLE Graph (\n" +
                "    src INT,\n" +
                "    dst INT\n" +
                ") WITH (\n" +
                "    'path' = 'examples/data/graph.dat'\n" +
                ")";
        SqlNodeList nodeList = SQLParser.parseDdl(ddl);
        CatalogManager catalogManager = new CatalogManager();
        catalogManager.register(nodeList);
        SqlPlanner crownPlanner = new SqlPlanner(catalogManager);
        RelNode root = crownPlanner.toLogicalPlan(tmp);
//        LogicalProject logicalProject=(LogicalProject) root;
//        LogicalFilter logicalFilter=(LogicalFilter) root.getInput(0);
//        System.out.println("LogicalProject:"+logicalProject);
//        System.out.println(logicalFilter.getCondition());
//        RexCall conditions= (RexCall) logicalFilter.getCondition();
//        RexNode c0=conditions.getOperands().get(4);
//        System.out.println(((RexCall)c0).getOperator().getName());
    }
}
