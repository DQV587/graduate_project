package graduateProject.planner.entity.hypergraph

trait HyperGraph[V, E <: HyperGraphEdge[V]] {

  val nodeSet: Set[V]
  val edgeSet: Set[E]

  def addNode(node: V): Unit = this.nodeSet + node

  def addEdge(edge: E): Unit = this.edgeSet + edge

  def getNodes: Set[V] = this.nodeSet

  def getEdges: Set[E] = this.edgeSet

  def isEmpty: Boolean = this.nodeSet.isEmpty

  def nonEmpty: Boolean = this.nodeSet.nonEmpty

  def getNodesNum: Int = this.nodeSet.size

  def getEdgesNum: Int = this.edgeSet.size

}
