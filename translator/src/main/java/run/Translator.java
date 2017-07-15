package run;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.Logger;

import tree.ProtobufJoinTree;
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
    
    private class NodeTriplePair {
        public ProtobufJoinTree.Node.Builder node;
        public Triple triple;

        public NodeTriplePair(ProtobufJoinTree.Node.Builder node, Triple triple){
            this.node = node;
            this.triple = triple;
        }
    }
    
    
    /*
     * buildTree constructs the JoinTree, ready to be serialized.
     */
    public Node buildTree() {
    	// sort the triples before adding them
    	//this.sortTriples();    	
    	
    	ArrayDeque<Triple> triplesQueue = new ArrayDeque<Triple> (triples);
    	
    	// create a builder (protobuf)
    	ProtobufJoinTree.Node.Builder treeBuilder = Node.newBuilder();
    	
    	// set the root node with the variables that need to be projected
    	for(int i = 0; i < variables.size(); i++)
    		treeBuilder.addProjection(variables.get(i).getVarName());
    	
    	// and add a triple to it
    	Triple currentTriple = triplesQueue.pop();
    	ProtobufJoinTree.Triple.Builder rootTriple = buildTriple(currentTriple);
    	treeBuilder.setTriple(rootTriple);

    	// visit the hypergraph to build the tree
    	ProtobufJoinTree.Node.Builder currentNode = treeBuilder;
    	ArrayDeque<NodeTriplePair> visitableNodes = new ArrayDeque<NodeTriplePair>();
    	while(triplesQueue.size() > 0){
    		
    		int limitWidth = 0;
    		String predicate = currentTriple.getPredicate().toString(prefixes);
    		// if a limit not set, a heuristic based on the predicate decides the width 
    		if(treeWidth == -1){
    	    	treeWidth = heuristicWidth(predicate); 
    		}
    		
    		
    		Triple newTriple = findRelateTriple(currentTriple, triplesQueue);
    		
    		// there are triples that are impossible to join with the current tree
    		if (newTriple == null) {
    			// set the limit to infinite and execute again
    			treeWidth = Integer.MAX_VALUE;
    			return buildTree();
    		}
    		
    		
    		// add every possible children (wide tree) or limit to a custom width
    		// stop if a width limit exists and is reached
    		while(newTriple != null && !(treeWidth > 0 && limitWidth == treeWidth)){
    			
    			// create the new child
    			ProtobufJoinTree.Node.Builder newChild = Node.newBuilder();
    			newChild.setTriple(buildTriple(newTriple));
    			
    			// append it to the current node and to the queue
    			currentNode.addChildren(newChild);
    			// get the proper builder
    			int i;
    			for (i = 0; i < currentNode.getChildrenCount(); i++)
    				if(currentNode.getChildrenBuilder(i).getTriple().equals(newChild.getTriple())) break;
    			visitableNodes.add(new NodeTriplePair(currentNode.getChildrenBuilder(i), newTriple));
    			
    			// remove consumed triple and look for another one
    			triplesQueue.remove(newTriple);
    			newTriple = findRelateTriple(currentTriple, triplesQueue);
    			
    			limitWidth++;
    		}
    		
    		// next Node is one of the children
    		if(!visitableNodes.isEmpty()){
    			NodeTriplePair currentPair = visitableNodes.pop();
    			currentNode = currentPair.node;
    			currentTriple = currentPair.triple;
    		}
    	}
    	
    	return treeBuilder.build();
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
     * findRelateTriple, given a source triple, finds another triple
     * with at least one variable in common, if there isn't return null
     */
    private Triple findRelateTriple(Triple triple, ArrayDeque<Triple> availableTriples){
    	if (!triple.getSubject().isVariable() && !triple.getObject().isVariable())
    		return null;
    	for(Triple t : availableTriples){
    		// check if one of the 4 cases is true (SS, SO, OS, OO)
    		if(t.getObject().isVariable() && (
    				t.getObject().equals(triple.getSubject()) || t.getObject().equals(triple.getObject())))
    			return t;
    		
    		if(t.getSubject().isVariable() && (
    				t.getSubject().equals(triple.getSubject()) || t.getSubject().equals(triple.getObject())))
    			return t;	
    	}
    	
    	return null;
    }
    
    /*
     * heuristicWidth decides a width based on the proportion
     * between the number of elements in a table and the unique subjects.
     */
    private int heuristicWidth(String predicate){
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
    
}
