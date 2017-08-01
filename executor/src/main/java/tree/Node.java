package tree;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import tree.ProtobufStats.Table;
import tree.Utils;

public class Node {
	public Triple triple;
	public List<Node> children;
	public List<String> projection;
	public List<Triple> tripleGroup;
	// the spark dataset containing the data relative to this node
	public Dataset<Row> sparkNodeData;
	
	// determine if is it worth to compute semi-joins for this node
	public boolean isReducible = false;
	
	
	/*
	 * The constructor recursively build the subTree 
	 * visiting the protobuf implementation
	 */
	public Node(ProtobufJoinTree.Node node){
		
		if(node.getTripleGroupCount() > 0){
			// set the triple group
			tripleGroup = new ArrayList<Triple>();
			for(ProtobufJoinTree.Triple t: node.getTripleGroupList()){
				tripleGroup.add(new Triple(t));
			}
		} else {
			// set the triple
			this.triple = new Triple(node.getTriple());
			this.tripleGroup = Collections.emptyList();
		}
		
		// set recursively the children
		this.children = new ArrayList<Node>();
		for(ProtobufJoinTree.Node child : node.getChildrenList()){
			 JoinTree subtreeChild = new JoinTree(child);
			 children.add(subtreeChild.getNode());
		}
		
		// decide if it is worth to use semi-joins reductions
		if (node.getTriple().hasStats()) {
			Table stats = node.getTriple().getStats();
			this.isReducible = stats.getSize() > stats.getDistinctSubjects();
		}
		// set the projections (if present)
		this.projection = node.getProjectionList();
		
	}
	
	
	/**
	 * computeNodeData sets the Dataset<Row> to the data referring to this node
	 */
	public void computeNodeData(SQLContext sqlContext){
		if(!tripleGroup.isEmpty()){
			computePropertyTableNodeData(sqlContext);
			return;
		}
		StringBuilder query = new StringBuilder("SELECT ");
		
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
		query.append(" FROM ");
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
	
	public void computePropertyTableNodeData(SQLContext sqlContext){
		
		StringBuilder query = new StringBuilder("SELECT ");
		ArrayList<String> whereConditions = new ArrayList<String>();		
		
		// subject
		query.append("s AS " + Utils.removeQuestionMark(tripleGroup.get(0).subject) + ",");

		// objects
		for(Triple t : tripleGroup){
			if (t.objectType == ElementType.CONSTANT){
				whereConditions.add(Utils.toMetastoreName(t.predicate) + "='" + t.object + "'");
			} else if (t.isComplex){
				query.append(" explode(" + Utils.toMetastoreName(t.predicate) + ") AS " + 
						Utils.removeQuestionMark(t.object) + ",");
			} else {
				query.append(" " + Utils.toMetastoreName(t.predicate) + " AS " +
						Utils.removeQuestionMark(t.object) + ",");
			} 
		}
		
		// delete last comma
		query.deleteCharAt(query.length() - 1);
		
		// TODO: parameterize the name of the table
		query.append(" FROM property_table ");
		if(!whereConditions.isEmpty()){
			query.append(" WHERE ");
			query.append(String.join(" AND ", whereConditions));
		}
		
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
			String joinVariable = Utils.findCommonVariable(triple, tripleGroup, child.triple, child.tripleGroup);
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
		// only if is reducible, compute semi-joins reductions
		if (isReducible) {
			for (Node child : children) {
				String commonVariable = Utils.findCommonVariable(triple, tripleGroup, child.triple, child.tripleGroup);
				this.sparkNodeData = this.sparkNodeData.join(
						child.sparkNodeData,
						this.sparkNodeData.col(commonVariable).equalTo(
								child.sparkNodeData.col(commonVariable)),
						"left_semi");
			}
		}
	}
	
	// reduce every child data by computing semi-joins with his father
	public void computeDownwardSemiJoin(SQLContext sqlContext){
		// only if is reducible, compute semi-joins reductions
		if (isReducible) {
			for (Node child : children) {
				String commonVariable = Utils.findCommonVariable(triple, tripleGroup, child.triple, child.tripleGroup);
				child.sparkNodeData = child.sparkNodeData.join(
						this.sparkNodeData,
						this.sparkNodeData.col(commonVariable).equalTo(
								child.sparkNodeData.col(commonVariable)),
						"left_semi");
			}
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