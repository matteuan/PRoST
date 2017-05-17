package run;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;

import run.JoinTree.Node;
import run.JoinTree.Node.Builder;

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
 * TODO: set the logger properly
 *
 * @author Matteo Cossu
 */
public class Translator {

    String inputFile;
    String outputFile;
    String statsPath;
    List<Var> variables;
    List<Triple> triples;
    
    public Translator(String input, String output, String statsPath) {
    	this.inputFile = input;
    	this.outputFile = output != null && output.length() > 0 ? output : input + ".out";
    	this.statsPath = statsPath;
    }
    
    public void translateQuery(){
    	
    	// parse the query and extract prefixes
        Query query = QueryFactory.read("file:"+inputFile);
        // TODO: consider prefixes
        PrefixMapping prefixes = query.getPrefixMapping();
        
        // extract variables and list of triples from the unique BGP
        OpProject opRoot = (OpProject) Algebra.compile(query);
        OpBGP singleBGP = (OpBGP) opRoot.getSubOp();
        variables = opRoot.getVars();
        triples = singleBGP.getPattern().getList();
        
        // build the tree and serialize it
        Node tree = buildTree();
        
        // output the file
        FileOutputStream fop;
        try {
			File file = new File(outputFile);
			fop = new FileOutputStream(file);

			if (!file.exists()) 
				file.createNewFile();
			
			tree.writeDelimitedTo(fop);
        } catch (IOException e) {
			e.printStackTrace();
		} 

    }
    
    private class NodeTriplePair {
        public JoinTree.Node.Builder node;
        public Triple triple;

        public NodeTriplePair(JoinTree.Node.Builder node, Triple triple){
            this.node = node;
            this.triple = triple;
        }
    }
    
    
    /*
     * buildTree constructs the JoinTree, ready to be serialized.
     */
    public Node buildTree() {
    	ArrayDeque<Triple> triplesQueue = new ArrayDeque<Triple> (triples);
    	
    	// create a builder (protobuf)
    	JoinTree.Node.Builder treeBuilder = Node.newBuilder();
    	
    	// set the root node with the variables that need to be projected
    	for(int i = 0; i < variables.size(); i++)
    		treeBuilder.addProjection(variables.get(i).getVarName());
    	
    	// and add a triple to it
    	Triple currentTriple = triplesQueue.pop();
    	JoinTree.Triple.Builder rootTriple = buildTriple(currentTriple);
    	treeBuilder.setTriple(rootTriple);

    	// visit the hypergraph to build the tree
    	JoinTree.Node.Builder currentNode = treeBuilder;
    	JoinTree.Node.Builder father = treeBuilder;
    	ArrayDeque<NodeTriplePair> visitableNodes = new ArrayDeque<NodeTriplePair>();
    	while(triplesQueue.size() > 0){
    		
    		// add every possible children (wide tree)
    		Triple newTriple = findRelateTriple(currentTriple, triplesQueue);
    		while(newTriple != null){
    			// create the new child
    			JoinTree.Node.Builder newChild = Node.newBuilder();
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
    private JoinTree.Triple.Builder buildTriple(Triple triple){
    	JoinTree.Triple.Builder tripleBuilder = JoinTree.Triple.newBuilder();
    	// extract and set the subject
    	if(triple.getSubject().isVariable())
    		tripleBuilder.setSubject(tripleBuilder.getSubjectBuilder()
        			.setName(triple.getSubject().getName())
        			.setType(JoinTree.Triple.ElementType.VARIABLE));
    	else
    		tripleBuilder.setSubject(tripleBuilder.getSubjectBuilder()
        			.setName(triple.getSubject().toString())
        			.setType(JoinTree.Triple.ElementType.CONSTANT));
    		
    	
    	// extract and set the predicate
    	tripleBuilder.setPredicate(tripleBuilder.getPredicateBuilder()
    			.setName(triple.getPredicate().toString())
    			.setType(JoinTree.Triple.ElementType.CONSTANT));
    	
    	// extract and set the object
    	if(triple.getObject().isVariable())
    		tripleBuilder.setObject(tripleBuilder.getObjectBuilder()
        			.setName(triple.getObject().getName())
        			.setType(JoinTree.Triple.ElementType.VARIABLE));
    	else
    		tripleBuilder.setObject(tripleBuilder.getObjectBuilder()
        			.setName(triple.getObject().toString())
        			.setType(JoinTree.Triple.ElementType.CONSTANT));
    	
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
    
}
