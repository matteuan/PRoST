package tree;



public class Triple {
	public String subject;
	public String predicate;
	public String object;
	public ElementType subjectType;
	public ElementType objectType;
	public ElementType predicateType;
	
	// construct from single properties
	public Triple(String subject, String predicate, String object, 
			ElementType subjectType, ElementType objectType, ElementType predicateType){
		this.subject = subject;
		this.subjectType = subjectType;
		this.predicate = predicate;
		this.predicateType = predicateType;
		this.object = object;
		this.objectType = objectType;
	}
	
	// construct from protobuf object
	public Triple(ProtobufJoinTree.Triple triple) {
		this.subject = triple.getSubject().getName();
		this.subjectType = convertElementType(triple.getSubject().getType());
		this.predicate = triple.getPredicate().getName();
		this.predicateType = convertElementType(triple.getPredicate().getType());
		this.object = triple.getObject().getName();
		this.objectType = convertElementType(triple.getObject().getType());
	}
	
	/*
	 * convertElementType converts the enum beloning to the protobuf serialization
	 * to this one
	 */
	public static ElementType 
	convertElementType(ProtobufJoinTree.Triple.ElementType t){
		if(t.equals(ProtobufJoinTree.Triple.ElementType.VARIABLE))
			return ElementType.VARIABLE;
		else
			return ElementType.CONSTANT;
	}
	
	@Override
	public String toString(){
		return String.format("(%s) (%s) (%s)", subject, predicate, object);
		
	}
}