package run;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

import tree.ProtobufJoinTree;
import tree.ProtobufJoinTree.Node.Builder;
import tree.Stats;
import tree.ProtobufJoinTree.Node;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;
/**
 * This class parses the SPARQL query,
 * build the Tree and save its serialization in a file.
 *
 * @author Matteo Cossu
 */
public class Translator {

    String inputFile;
    String outputFile;
    String statsFile;
    Stats stats;
    boolean statsActive = false;
    int treeWidth;
    PrefixMapping prefixes;
    List<Var> variables;
    List<Triple> triples;
	private boolean usePropertyTable;
    private static final Logger logger = Logger.getLogger(Main.class);
    
    public Translator(String input, String output, String statsPath, int treeWidth) {
    	this.inputFile = input;
    	this.outputFile = output != null && output.length() > 0 ? output : input + ".out";
    	this.statsFile = statsPath;
    	if(statsFile.length() > 0){
    		stats = new Stats(statsFile);
    		statsActive = true;
    	}
    	this.treeWidth = treeWidth;
    }
    
    public void translateQuery(){
    	
    	// parse the query and extract prefixes
        Query query = QueryFactory.read("file:"+inputFile);
        // TODO: consider prefixes
        prefixes = query.getPrefixMapping();
        
        logger.info("*** SPARQL QUERY ***\n" + query +"\n********************"  );
        
        // extract variables and list of triples from the unique BGP
        OpProject opRoot = (OpProject) Algebra.compile(query);
        OpBGP singleBGP = (OpBGP) opRoot.getSubOp();
        variables = opRoot.getVars();
        triples = singleBGP.getPattern().getList();
        
        // build the tree and serialize it
        Node tree = buildTree();
        logger.info("*** Spark JoinTree ***\n" + tree +"\n********************" );
        
        // output the file
        FileOutputStream fop;
        try {
			File file = new File(outputFile);
			fop = new FileOutputStream(file);

			if (!file.exists()) 
				file.createNewFile();
			
			tree.writeTo(fop);
        } catch (IOException e) {
        	logger.error("Impossible to write the output file");
			e.printStackTrace();
			return;
		} 
        
        logger.info("Output file written with success");
    }
    
    
    /*
     * buildTree constructs the JoinTree, ready to be serialized.
     */
    public Node buildTree() {
    	// sort the triples before adding them
    	//this.sortTriples();    	
    	
    	PriorityQueue<Node.Builder> nodesQueue = getNodesQueue();
    	
    	Node.Builder treeBuilder = nodesQueue.poll();
    	
    	// set the root node with the variables that need to be projected
    	for(int i = 0; i < variables.size(); i++)
    		treeBuilder.addProjection(variables.get(i).getVarName());

    	// visit the hypergraph to build the tree
    	ProtobufJoinTree.Node.Builder currentNode = treeBuilder;
    	ArrayDeque<Node.Builder> visitableNodes = new ArrayDeque<Node.Builder>();
    	while(!nodesQueue.isEmpty()){
    		
    		int limitWidth = 0;
    		// if a limit not set, a heuristic decides the width 
    		if(treeWidth == -1){
    	    	treeWidth = heuristicWidth(currentNode); 
    		}
    		
    		//Triple newTriple = findRelateTriple(currentTriple, triplesQueue);
    		Node.Builder newNode =  findRelateNode(currentNode, nodesQueue);
    		
    		// there are nodes that are impossible to join with the current tree width
    		if (newNode == null && visitableNodes.isEmpty()) {
    			// set the limit to infinite and execute again
    			treeWidth = Integer.MAX_VALUE;
    			return buildTree();
    		}
    		
    		// add every possible children (wide tree) or limit to a custom width
    		// stop if a width limit exists and is reached
    		while(newNode != null && !(treeWidth > 0 && limitWidth == treeWidth)){
    			
    			// append it to the current node and to the queue
    			currentNode.addChildren(newNode);
    			
    			// get the proper builder
    			int i = 0;
    			for (i = 0; i < currentNode.getChildrenCount(); i++)
    				if(currentNode.getChildrenBuilder(i).equals(newNode)) break;
    			visitableNodes.add(currentNode.getChildrenBuilder(i - 1));
    			
    			// remove consumed node and look for another one
    			nodesQueue.remove(newNode);
    			newNode = findRelateNode(currentNode, nodesQueue);
    			
    			limitWidth++;
    		}
    		
    		// next Node is one of the children
    		if(!visitableNodes.isEmpty() && !nodesQueue.isEmpty()){
    			currentNode = visitableNodes.pop();
    			
    		}
    	}
    	
    	return treeBuilder.build();
    }
        
    
    private PriorityQueue<Builder> getNodesQueue() {
    	PriorityQueue<Builder> nodesQueue = new PriorityQueue<ProtobufJoinTree.Node.Builder>
    		(triples.size(), new NodeComparator(this.stats));
    	if(usePropertyTable){
			HashMap<String, List<Triple>> subjectGroups = new HashMap<String, List<Triple>>();
			
			// group by subjects
			for(Triple triple : triples){
				String subject = triple.getSubject().toString(prefixes);
		
				if (subjectGroups.containsKey(subject)) {
					subjectGroups.get(subject).add(triple);
				} else {
					List<Triple> subjTriples = new ArrayList<Triple>();
					subjTriples.add(triple);
					subjectGroups.put(subject, subjTriples);
				}
			}
			
			// create and add the proper nodes
			for(String subject : subjectGroups.keySet()){
				if (subjectGroups.get(subject).size() > 1){
					nodesQueue.add(buildNode(null, subjectGroups.get(subject)));
				} else {
					nodesQueue.add(buildNode(subjectGroups.get(subject).get(0), Collections.<Triple> emptyList()));
				}
			}			
    
		} else {
			for(Triple t : triples){
				nodesQueue.add(buildNode(t, Collections.<Triple> emptyList()));
			}
		}
    	return nodesQueue;
	}

	private ProtobufJoinTree.Node.Builder buildNode(Triple triple, List<Triple> tripleGroup){
    	ProtobufJoinTree.Node.Builder nodeBuilder = ProtobufJoinTree.Node.newBuilder();
    	
    	if(tripleGroup.isEmpty())
    		nodeBuilder.setTriple(buildTriple(triple));
    	else{
    		
    		for(int i = 0; i < tripleGroup.size(); i++){
    			//nodeBuilder.setTripleGroup(i, buildTriple(tripleGroup.get(i)));
    			nodeBuilder.addTripleGroup(buildTriple(tripleGroup.get(i)));
    		}
    	}
    	
    	return nodeBuilder;
    }
    
    
    /*
     * buildTriple takes as input a Jena Triple
     * and produces a builder of triples belonging to JoinTree type
     */
    private ProtobufJoinTree.Triple.Builder buildTriple(Triple triple){
    	ProtobufJoinTree.Triple.Builder tripleBuilder = ProtobufJoinTree.Triple.newBuilder();
    	// extract and set the subject
    	if(triple.getSubject().isVariable())
    		tripleBuilder.setSubject(tripleBuilder.getSubjectBuilder()
        			.setName(triple.getSubject().toString(prefixes))
        			.setType(ProtobufJoinTree.Triple.ElementType.VARIABLE));
    	else
    		tripleBuilder.setSubject(tripleBuilder.getSubjectBuilder()
        			.setName(triple.getSubject().toString(prefixes))
        			.setType(ProtobufJoinTree.Triple.ElementType.CONSTANT));
    		
    	
    	// extract and set the predicate
    	tripleBuilder.setPredicate(tripleBuilder.getPredicateBuilder()
    			.setName(triple.getPredicate().toString(prefixes))
    			.setType(ProtobufJoinTree.Triple.ElementType.CONSTANT));
    	
    	// extract and set the object
    	if(triple.getObject().isVariable())
    		tripleBuilder.setObject(tripleBuilder.getObjectBuilder()
        			.setName(triple.getObject().toString(prefixes))
        			.setType(ProtobufJoinTree.Triple.ElementType.VARIABLE));
    	else
    		tripleBuilder.setObject(tripleBuilder.getObjectBuilder()
        			.setName(triple.getObject().toString(prefixes))
        			.setType(ProtobufJoinTree.Triple.ElementType.CONSTANT));
    	
		// set optional statistics
		if(statsActive) tripleBuilder.setStats(stats.getTableStats(
				triple.getPredicate().toString(prefixes)));
		
    	return tripleBuilder;
    }
    
    
    /*
     * findRelateNode, given a source node, finds another node
     * with at least one variable in common, if there isn't return null
     * TODO: clean this mess
     */
    private Node.Builder findRelateNode(Node.Builder sourceNode, PriorityQueue<Node.Builder> availableNodes){
    	
    	if (sourceNode.getTripleGroupCount() > 0){
    		// sourceNode is a group
    		for(tree.ProtobufJoinTree.Triple tripleSource : sourceNode.getTripleGroupList()){
				for (Node.Builder node : availableNodes){
					if(node.getTripleGroupCount() > 0) {
						for(tree.ProtobufJoinTree.Triple tripleDest : node.getTripleGroupList())
	    					if(existsVariableInCommon(tripleSource, tripleDest))
	    						return node;
					} else {
						if(existsVariableInCommon(tripleSource, node.getTriple()))
							return node;
					}
				}
    		}
    		
    	} else {
    		// source node is not a group
    		for (Node.Builder node : availableNodes) {
    			if(node.getTripleGroupCount() > 0) {
    				for(tree.ProtobufJoinTree.Triple tripleDest : node.getTripleGroupList()){
    					if(existsVariableInCommon(tripleDest, sourceNode.getTriple()))
    						return node;
    				}
    			} else {
    				if(existsVariableInCommon(sourceNode.getTriple(), node.getTriple()))
    					return node;
    			}
    		}
    	}
    	return null;
    }
    
    private boolean existsVariableInCommon(Triple a, Triple b) {
    	if(a.getObject().isVariable() && (
    			a.getObject().equals(b.getSubject()) || a.getObject().equals(b.getObject())))
    		return true;
    	
    	if(a.getSubject().isVariable() && (
    			a.getSubject().equals(b.getSubject()) || a.getSubject().equals(b.getObject())))
    		return true;
    	
    	return false;
    }
    
    private boolean existsVariableInCommon(ProtobufJoinTree.Triple a, ProtobufJoinTree.Triple b) {
    	ProtobufJoinTree.Triple.ElementType variableType = ProtobufJoinTree.Triple.ElementType.VARIABLE;
    	if(a.getObject().getType() == variableType && (
    			a.getObject().equals(b.getSubject()) || a.getObject().equals(b.getObject()))) 
    		return true;
		
    	if(a.getSubject().getType() == variableType && (
    			a.getSubject().equals(b.getSubject()) || a.getSubject().equals(b.getObject())))
    		return true;
    	
		return false;
    }
    
    /*
     * heuristicWidth decides a width based on the proportion
     * between the number of elements in a table and the unique subjects.
     */
    private int heuristicWidth(Node.Builder node){
    	if(!node.getTripleGroupList().isEmpty())
    		return 5;
    	String predicate = node.getTriple().getPredicate().getName();
    	int tableSize = stats.getTableSize(predicate);
    	int numberUniqueSubjects = stats.getTableDistinctSubjects(predicate);
    	float proportion = tableSize / numberUniqueSubjects;
    	if(proportion > 1)
    		return 3;
    	return 2;
    }
    
    
    /*
     * Simple triples reordering based on statistics.
     * Thanks to this method the building of the tree will follow a better order.
     */
    private void sortTriples(){
    	if(triples.size() == 0 || !statsActive) return;
    	
    	logger.info("Triples being sorted");
    	
    	// find the best root
    	int indexBestRoot = 0;
    	String predicate = triples.get(0).getPredicate().toString(prefixes);
    	int bestSize = stats.getTableSize(predicate);
    	float bestProportion = bestSize / stats.getTableDistinctSubjects(predicate);
    	for(int i = 1; i < triples.size(); i++){
    		predicate = triples.get(i).getPredicate().toString(prefixes);
        	float proportion = stats.getTableSize(predicate) / 
        			stats.getTableDistinctSubjects(predicate);
        	
        	// update best if the proportion is better
        	if (proportion > bestProportion){
        		indexBestRoot = i;
        		bestProportion = proportion;
        		bestSize = stats.getTableSize(predicate); 
        	} // or if the table size is bigger
        	else if (proportion == bestProportion && stats.getTableSize(predicate) > bestSize) {
        		indexBestRoot = i;
        		bestSize = stats.getTableSize(predicate);        		
        	}
    	}

    	// move the best triple to the front
    	if(indexBestRoot > 0){
    		Triple tripleToMove = triples.get(indexBestRoot);
    		triples.remove(indexBestRoot);
    		triples.add(0, tripleToMove);
    	}
    	  	
    }

	public void setPropertyTable(boolean b) {
		this.usePropertyTable = b;
		
	}
    
}
