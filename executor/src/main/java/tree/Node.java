package tree;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.ColumnName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import tree.Utils;

public class Node {
	public Triple triple;
	public List<Node> children;
	public List<String> projection;
	public Dataset<Row> sparkNodeData;
	
	
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
	 * computeNodeData sets the Dataset<Row> to the data referring to this node
	 */
	public void computeNodeData(SQLContext sqlContext){
		StringBuilder query = new StringBuilder("SELECT ");
		
//		if (projection != null && projection.size() > 0){
//			if(projection.size() == 2){
//				query.append("s AS " + Utils.removeQuestionMark(projection.get(1))+ ", ");
//				query.append("o AS " + Utils.removeQuestionMark(projection.get(0)) + " ");
//			} else {
//				if (projection.get(0).equals(triple.subject))
//					query.append("s AS " + Utils.removeQuestionMark(projection.get(1)) + " ");
//				else {
//					query.append("o AS " + Utils.removeQuestionMark(projection.get(0)) + " ");
//				}
//			}
//		} else { // if names are not set, select only the variables
		
		// SELECT
		if (triple.subjectType == ElementType.VARIABLE &&
				triple.objectType == ElementType.VARIABLE)
			query.append("s AS " + Utils.removeQuestionMark(triple.subject) + 
					", o AS " + Utils.removeQuestionMark(triple.object) + " ");
		else if (triple.subjectType == ElementType.VARIABLE)
			query.append("s AS " + Utils.removeQuestionMark(triple.subject) );
		else if (triple.objectType == ElementType.VARIABLE) 
			query.append("o AS " + Utils.removeQuestionMark(triple.object));
	
			
		// FROM
		query.append("FROM ");
		query.append("vp_" + tree.Utils.toMetastoreName(triple.predicate));
		
		
		// WHERE
		if( triple.objectType == ElementType.CONSTANT || triple.subjectType == ElementType.CONSTANT)
			query.append(" WHERE ");
		if (triple.objectType == ElementType.CONSTANT)
			query.append(" o='" + triple.object +"' ");
		
		if (triple.subjectType == ElementType.CONSTANT)
			query.append(" o='" + triple.subject +"' ");
		
		this.sparkNodeData = sqlContext.sql(query.toString());
	}
	
	// call computeNodeData recursively on the whole subtree
	public void computeSubTreeData(SQLContext sqlContext){
		this.computeNodeData(sqlContext);
		for(Node child: this.children)
			child.computeSubTreeData(sqlContext);
	}
	
	// join tables between itself and all the children
	public Dataset<Row> computeJoinWithChildren(SQLContext sqlContext){
		if (sparkNodeData == null)
			this.computeNodeData(sqlContext);
		Dataset<Row> currentResult = this.sparkNodeData;
		for (Node child: children){
			Dataset<Row> childResult = child.computeJoinWithChildren(sqlContext);
			String joinVariable = tree.Utils.findCommonVariable(this.triple, child.triple);
			if (joinVariable != null)
				currentResult = currentResult.join(childResult, joinVariable);
			
		}
		return currentResult;
	}
	
	// reduce every father data by computing semi-joins with children
	public void computeUpwardSemiJoin(SQLContext sqlContext){
		for(Node child: children){
			child.computeUpwardSemiJoin(sqlContext);
		}
		for(Node child: children){
			String commonVariable = Utils.findCommonVariable(triple, child.triple);
			this.sparkNodeData = this.sparkNodeData.join(child.sparkNodeData, 
					this.sparkNodeData.col(commonVariable).equalTo(child.sparkNodeData.col(commonVariable)), "left_semi");
			this.sparkNodeData.join(child.sparkNodeData, 
					this.sparkNodeData.col(commonVariable).equalTo(child.sparkNodeData.col(commonVariable)), "left_semi").explain();
		}
	}
	
	// reduce every child data by computing semi-joins with his father
	public void computeDownwardSemiJoin(SQLContext sqlContext){
		for(Node child: children){
			String commonVariable = Utils.findCommonVariable(triple, child.triple);
			child.sparkNodeData = child.sparkNodeData.join(this.sparkNodeData, 
					this.sparkNodeData.col(commonVariable).equalTo(child.sparkNodeData.col(commonVariable)), "left_semi");
		}
		for(Node child: children){
			child.computeDownwardSemiJoin(sqlContext);
		}
	}
	
	@Override
	public String toString(){
		StringBuilder str = new StringBuilder("[Triple: " + triple.toString() + " Children: ");
		for (Node child: children){
			str.append(child.toString() + "\t" );
		}
		str.append("]");
		return str.toString();
	}
}