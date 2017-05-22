package tree;



public class Utils {
	
	
	/**
	 * Makes the string conform to the requirements for HiveMetastore column names.
	 * e.g. remove braces, replace non word characters, trim spaces.
	 */
	public static String toMetastoreName(String s) {
		return s.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
	}

}
