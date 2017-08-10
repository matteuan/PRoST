package run;

import java.util.Comparator;

import tree.ProtobufJoinTree.Node;
import tree.ProtobufJoinTree.Node.Builder;
import tree.ProtobufJoinTree.Triple.ElementType;
import tree.Stats;

public class NodeComparator implements Comparator<Node.Builder> {
	
	private Stats stats;
	
	
	
	public NodeComparator(Stats stats) {
		this.stats = stats;
	}



	public float heuristicNodePriority(Builder node){
		
		float priority;
		
		if(node.getTripleGroupCount() > 0) {
			priority = 1 / node.getTripleGroupCount();
		} else {
			String predicate = node.getTriple().getPredicate().getName();
			boolean isObjectVariable = node.getTriple().getObject().getType() == ElementType.VARIABLE;
			boolean isSubjectVariable = node.getTriple().getSubject().getType() == ElementType.VARIABLE;
			if (!isObjectVariable || !isSubjectVariable){
				priority = 0;
			} else {
				int size = stats.getTableSize(predicate);	
				int distinctSubjects = stats.getTableDistinctSubjects(predicate);
				priority = (float) (size * (Math.log(distinctSubjects) / Math.log(1.2)));
			}
		}
		
		return priority;
	}
	
	@Override
	public int compare(Builder node1, Builder node2) {
		
		float priorityNode1 = heuristicNodePriority(node1);
		float priorityNode2 = heuristicNodePriority(node2);
	
		return (int) Math.ceil(priorityNode1 - priorityNode2);
	}

}
