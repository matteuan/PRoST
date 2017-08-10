package tree;

import java.util.List;



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
	private static String findCommonVariable(Triple a, Triple b){
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
	
	public static String findCommonVariable(Triple tripleA, List<Triple> tripleGroupA, 
			Triple tripleB, List<Triple> tripleGroupB){
		// triple with triple case
		if(tripleGroupA.isEmpty() && tripleGroupB.isEmpty())
			return findCommonVariable(tripleA, tripleB);
		if(!tripleGroupA.isEmpty() && !tripleGroupB.isEmpty())
			for(Triple at : tripleGroupA)
				for(Triple bt : tripleGroupB)
					if(findCommonVariable(at, bt) != null)
						return findCommonVariable(at, bt);
		if(tripleGroupA.isEmpty())
			for(Triple bt : tripleGroupB)
				if(findCommonVariable(tripleA, bt) != null)
					return findCommonVariable(tripleA, bt);
		if(tripleGroupB.isEmpty())
			for(Triple at : tripleGroupA)
				if(findCommonVariable(at, tripleB) != null)
					return findCommonVariable(at, tripleB);
		
		return null;
	}

}
