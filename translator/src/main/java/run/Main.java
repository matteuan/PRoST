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
 * -s, --stats <file> File with optional statistics.
 * -i, --input <file> SPARQL query file to translate 
 * -o, --output <file> Specify the filename with the resulting tree.
 * -w, --width <number> the maximum Tree width
 * -p, --propertytable If set, the translator will create nodes for the property table
 * 
 * @author Matteo Cossu
 */
public class Main {
	private static String inputFile;
	private static String outputFile;
	private static String statsFileName = "";
	private static final Logger logger = Logger.getLogger(Main.class);
	private static int treeWidth = -1;
	private static boolean usePropertyTable = false;
	
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
		Option statOpt = new Option("s", "stats", true, "File with optional statistics");
		options.addOption(statOpt);
		Option helpOpt = new Option("h", "help", true, "Print this help.");
		options.addOption(helpOpt);
		Option widthOpt = new Option("w", "width", true, "The maximum Tree width");
		options.addOption(widthOpt);
		Option propertyTableOpt = new Option("p", "propertytable", false, "Use also property table");
		options.addOption(propertyTableOpt);
		
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
			statsFileName = cmd.getOptionValue("stats");
			logger.info("VP statistics are being used.");
		}
		if(cmd.hasOption("width")){
			treeWidth = Integer.valueOf(cmd.getOptionValue("width"));
			logger.info("Maximum tree width is set to " + String.valueOf(treeWidth));
		}
		if(cmd.hasOption("propertytable")){
			usePropertyTable = true;
			logger.info("Using property table additional option");
		}
		
		/*
		 * Translation Phase
		 */
		Translator translator = new Translator(inputFile, outputFile, statsFileName, treeWidth);
		if (usePropertyTable) translator.setPropertyTable(true);
		translator.translateQuery();
	}

}
