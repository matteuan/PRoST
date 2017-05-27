package tree;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


/**
 * JoinTree is a wrapper around the ProtobufJoinTree class
 * @author Matteo Cossu
 *
 */
public class JoinTree {
	
	private Node node;
	
	public Node getNode() {
		return node;
	}

	
	public JoinTree(ProtobufJoinTree.Node node){
		this.node = new Node(node);
	}
	
	public void computeSingularNodeData(SQLContext sqlContext){
		node.computeSubTreeData(sqlContext);		
	}
	
	public Dataset<Row> computeJoins(SQLContext sqlContext){
		// compute all the joins
		Dataset<Row> results = node.computeJoinWithChildren(sqlContext);
		// select only the requested result
		String [] selectedColumns = new String[node.projection.size()];
		for (int i = 0; i < selectedColumns.length; i++) 
			selectedColumns[i]= node.projection.get(i);
		results.selectExpr(selectedColumns);
		
		return results;
		
	}
	
	
	// TODO: improve this
	@Override
	public String toString(){
		StringBuilder str = new StringBuilder("JoinTree \n");
		str.append(node.toString());
		for (Node child: node.children){
			str.append("\t" + child.toString());
		}
		
		return str.toString();
	}

}
