package tree;


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
