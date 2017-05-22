import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.hive.HiveContext;

import tree.JoinTree;
import tree.ProtobufJoinTree;

/**
 * This class parses the JoinTree,
 * and execute the distributed Yannakakis algorithm (GYM)
 * on top of Spark .
 *
 * @author Matteo Cossu
 */
public class Executor {
	String inputFile;
	String outputFile;
	String databaseName;
	JoinTree convertedTree;
	SparkContext sc;
	HiveContext sqlContext;
	SparkConf conf;

	private static final Logger logger = Logger.getLogger(Main.class);
	
	public Executor(String inputFile, String outputFile, String databaseName){
		this.inputFile = inputFile;
		this.outputFile = outputFile;
		this.databaseName = databaseName;
		
		// initialize the Spark environment 
		this.conf = new SparkConf().setAppName("SparkVP-Executor");
		this.sc = new SparkContext(conf);
		this.sqlContext =  new HiveContext(sc);
	}
	
	/*
	 * parseTree reads the input file and it transforms the JoinTree
	 * in a Spark execution plan
	 */
	public void parseTree() {
		ProtobufJoinTree.Node root = null;
		try {
			root = ProtobufJoinTree.Node.parseFrom(
					new FileInputStream(inputFile));
		} catch (FileNotFoundException e) {
			logger.error("Input File Not Found");
			return;
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		convertedTree = new JoinTree(root);
		logger.info("JoinTree correctly parsed");
	}
	
	
	/*
	 * execute performs the Spark computation and measure the time required
	 */
	public void execute() {
		if(convertedTree == null){
			logger.error("The JoinTree was not correctly parsed");
			return;
		}
		
		
	}

}
