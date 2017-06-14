

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;


/**
 * The Main class parses the CLI arguments and calls the executor.
 * 
 * Options: 
 * -h, --help prints the usage help message.
 * -d, --DB <database> Database containing the VP tables
 * -i, --input <file> JoinTree input file, representing the query
 * -o, --output <HDFSfile> Optional HDFS output where to save the results
 * 
 * @author Matteo Cossu
 */
public class Main {
	private static String inputFile;
	private static String outputDB;
	private static String databaseName;
	private static final Logger logger = Logger.getLogger(Main.class);
	public static void main(String[] args) {
		
		/*
		 * Manage the CLI options
		 */
		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		Option inputOpt = new Option("i", "input", true, "JoinTree input file, representing the query.");
		inputOpt.setRequired(true);
		options.addOption(inputOpt);
		Option outputOpt = new Option("o", "output", true, "Optional output DB name.");
		options.addOption(outputOpt);
		Option databaseOpt = new Option("d", "DB", true, "Database containing the VP tables.");
		databaseOpt.setRequired(true);
		options.addOption(databaseOpt);
		Option helpOpt = new Option("h", "help", true, "Print this help.");
		options.addOption(helpOpt);
		
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch(MissingOptionException e){
			 formatter.printHelp("JAR", "Execute a JoinTre on Spark", options, "", true);
			 return;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		if(cmd.hasOption("help")){
			formatter.printHelp("JAR", "Translate a SPARQL query in a Spark Plan", options, "", true);
			return;
		}
		if(cmd.hasOption("input")){
			inputFile = cmd.getOptionValue("input");
		}
		if(cmd.hasOption("output")){
			outputDB = cmd.getOptionValue("output");
			logger.info("Output database set to: " + outputDB);
		}
		if(cmd.hasOption("DB")){
			databaseName = cmd.getOptionValue("DB");
			logger.info("Input database set to: " + databaseName);
		}
		
		File file = new File(inputFile);
		
		if(file.isFile()){
			Executor executor = new Executor(inputFile, outputDB, databaseName);
			executor.parseTree();
			executor.execute();
		} else if(file.isDirectory()){
			// if the path is a directory execute every files inside
			for(String filename : file.list()){
				Executor executor = new Executor(filename, outputDB, databaseName);
				executor.parseTree();
				executor.execute();
			}
		}
	}

}
