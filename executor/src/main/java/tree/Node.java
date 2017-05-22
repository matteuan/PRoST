package tree;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class Node {
	Triple triple;
	List<Node> children;
	List<String> projection;
	DataFrame sparkNodeData;
	
	
	/*
	 * The constructor recursively build the subTree 
	 * visiting the protobuf implementation
	 */
	public Node(ProtobufJoinTree.Node node){
		
		// set the triple
		this.triple = new Triple(node.getTriple());
		
		// set recursively the children
		this.children = new ArrayList<Node>();
		for(ProtobufJoinTree.Node child : node.getChildrenList()){
			 JoinTree subtreeChild = new JoinTree(child);
			 children.add(subtreeChild.getNode());
		}
		
		// set the projections (if present)
		this.projection = node.getProjectionList();
		
	}
	
	
	/**
	 * computeNodeData sets the DataFrame to the data referring to this node
	 */
	public void computeNodeData(HiveContext sqlContext){
		StringBuilder query = new StringBuilder("SELECT ");
		
		// SELECT
		
		// if projection names are set, use them
		if (projection != null && projection.size() > 0){
			if(projection.size() == 2){
				query.append("s AS '" + projection.get(0) + "', ");
				query.append("o AS '" + projection.get(1) + "' ");
			} else {
				if (projection.get(0).equals(triple.subject))
					query.append("s AS '" + projection.get(0) + "' ");
				else {
					query.append("o AS '" + projection.get(1) + "' ");
				}
			}
		} else { // if names are not set, select only the variables
			if (triple.subjectType == ElementType.VARIABLE &&
					triple.objectType == ElementType.VARIABLE)
				query.append("s, o ");
			else if (triple.subjectType == ElementType.VARIABLE)
				query.append("s ");
			else if (triple.objectType == ElementType.VARIABLE) 
				query.append("o ");
		}
			
		// FROM
		query.append("FROM ");
		query.append(tree.Utils.toMetastoreName(triple.predicate));
		
		
		// WHERE
		if( triple.objectType == ElementType.CONSTANT || triple.subjectType == ElementType.CONSTANT)
			query.append(" WHERE ");
		if (triple.objectType == ElementType.CONSTANT)
			query.append(" o='" + triple.object +"' ");
		
		if (triple.subjectType == ElementType.CONSTANT)
			query.append(" o='" + triple.subject +"' ");
		
		this.sparkNodeData = sqlContext.sql(query.toString());
	}
	
	@Override
	public String toString(){
		StringBuilder str = new StringBuilder("[Triple:\n" + triple.toString() + "\n Children:\n");
		for (Node child: children){
			str.append(child.toString());
		}
		str.append("]");
		return str.toString();
	}
}