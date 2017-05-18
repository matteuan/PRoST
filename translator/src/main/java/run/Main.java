package run;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;


/**
 * The Main class parses the CLI arguments and calls the translator.
 * 
 * Options: 
 * -h, --help prints the usage help message.
 * -s, --stats <path> Folder with optional statistics.
 * -i, --input <file> SPARQL query file to translate 
 * -o, --output <file> Specify the filename with the resulting tree.
 * 
 * @author Matteo Cossu
 */
public class Main {
	private static String inputFile;
	private static String outputFile;
	private static String statsPath;
	private static final Logger logger = Logger.getLogger(Main.class);
	
	public static void main(String[] args) {
		
		/*
		 * Manage the CLI options
		 */
		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		Option inputOpt = new Option("i", "input", true, "Input file with the SPARQL query.");
		inputOpt.setRequired(true);
		options.addOption(inputOpt);
		Option outputOpt = new Option("o", "output", true, "Custom output filename");
		options.addOption(outputOpt);
		Option statOpt = new Option("s", "stats", true, "Folder with optional statistics");
		options.addOption(statOpt);
		Option helpOpt = new Option("h", "help", true, "Print this help.");
		options.addOption(helpOpt);
		
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch(MissingOptionException e){
			 formatter.printHelp("JAR", "Translate a SPARQL query in a Spark Plan", options, "", true);
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
			outputFile = cmd.getOptionValue("output");
			logger.info("Output file set to:" + outputFile);
		}
		if(cmd.hasOption("stats")){
			statsPath = cmd.getOptionValue("stats");
			logger.info("VP statistics are being used.");
		}
		
		/*
		 * Translation Phase
		 */
		Translator translator = new Translator(inputFile, outputFile, statsPath);
		translator.translateQuery();
	}

}
