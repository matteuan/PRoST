package tree;



public class Utils {
	
	
	/**
	 * Makes the string conform to the requirements for HiveMetastore column names.
	 * e.g. remove braces, replace non word characters, trim spaces.
	 */
	public static String toMetastoreName(String s) {
		return s.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
	}
	
	public static String removeQuestionMark(String s){
		if(s.startsWith("?"))
			return s.substring(1);
		return s;
	}
	
	/**
	 * findCommonVariable find a return the common variable between two triples.
	 * 
	 */
	public static String findCommonVariable(Triple a, Triple b){
		if(a.subjectType == ElementType.VARIABLE && 
				(removeQuestionMark(a.subject).equals(removeQuestionMark(b.subject)) 
						|| removeQuestionMark(a.subject).equals(removeQuestionMark(b.object))))
			return removeQuestionMark(a.subject);
		if(a.objectType == ElementType.VARIABLE && 
				(removeQuestionMark(a.object).equals(removeQuestionMark(b.subject)) 
						|| removeQuestionMark(a.object).equals(removeQuestionMark(b.object))))
			return removeQuestionMark(a.object);
		return null;
	}

}
