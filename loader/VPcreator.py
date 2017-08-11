import sys, re, argparse, subprocess, os
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import DataFrameWriter
import pyspark.sql.functions as f
import Stats_pb2


"""
valid_string converts in '_' all the characters that are not
accepted as table names in the hive metastore. 
"""
def valid_string(s):
    return re.sub(r"[^\w]", "_", s)


class Stats:
    """
    Stats collects statistics during the loading and save them separately in a file. 
    """

    def __init__(self):
        self.graph = Stats_pb2.Graph()

    def addTableStat(self, property, df):
        tableStats = self.graph.tables.add()
        tableStats.name = property
        tableStats.size = df.count()
        tableStats.distinctSubjects = df.select("s").distinct().count()
    
    def getSerializedStats(self):
        return self.graph.SerializeToString()

class VP_creator:
    """ 
    VP_creator creates for an input RDF dataset
    the corresponding Triple Table, Vertical Partitioning tables (VP)
    """

    def __init__(self, sc, sqlContext, inputFile, outputDB, statsFile):
        self.sc = sc
        self.sqlContext = sqlContext
        self.inputFile = inputFile
        self.outputDB = outputDB
        self.properties = {}
        self.sc.setLogLevel("WARN")
        self.statsFile = statsFile
        self.statsEnabled = statsFile != ""
        self.stats = Stats()
        self.property_table_jar = ""
        self.property_table_enabled = False
        
        # from now on, use the proper DB
        sqlContext.sql("CREATE DATABASE IF NOT EXISTS " + self.outputDB)
        sqlContext.sql("USE " + self.outputDB)

    def create_triple_table(self):
        triple_table = self.sqlContext.sql(
            "CREATE EXTERNAL TABLE IF NOT EXISTS tripletable (s STRING, p STRING, o STRING) ROW FORMAT DELIMITED" \
            + " FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' LOCATION '"+ self.inputFile +"'")
        print "Triple Table created."
    
    def extract_properties(self):
        # we assume that the number of properties is small
        for p in self.sqlContext.sql('SELECT DISTINCT p FROM tripletable').collect():
            self.properties[p["p"]] = True
        print "Properties Extracted. There are %(num)d properties" % {"num": len(self.properties)}

    def create_VP_tables(self):
        print "Beginning the creation of VP tables."
        total_properties = len(self.properties)
        i = 0
        # for each distinct property, create a table
        for p in self.properties:
            i += 1
            prop_df = self.sqlContext.sql("SELECT s AS s, o AS o FROM tripletable WHERE p='" + p + "'")
            df_writer = DataFrameWriter(prop_df)
            df_writer.saveAsTable("VP_" + valid_string(p))
            sys.stdout.write("\rTables created: %d / %d " % (i, total_properties))
        
        # if statistics are enabled, compute them
        if self.statsEnabled:
            i = 0
            stat = Stats()
            for p in self.properties:
                i += 1
                tableDF = self.sqlContext.sql("SELECT * FROM VP_" + valid_string(p))
                stat.addTableStat(p, tableDF)
                sys.stdout.write("\rStatistics created: %d / %d " % (i, total_properties))
            with open(self.statsFile, "w") as f:
                f.write(stat.getSerializedStats())
        print "Statistics created: %d / %d "  % (i, total_properties)
    
    def set_propertytable_jar(self, PTjar):
        self.property_table_enabled = True
        self.property_table_jar = PTjar
    
    def create_property_table(self):
        # TODO set the thing only with python
        translate_command =  'spark2-submit --driver-memory 3G --jars ../executor/commons-cli-1.3.1.jar  --conf "spark.driver.userClassPathFirst=true" --class Main ' + self.property_table_jar + " -i " + self.inputFile + " -o " + self.outputDB
        print translate_command
        execution_output = subprocess.check_output(['bash','-c', translate_command]) 
        print execution_output
        print "Property table loaded"


    def run_creator(self):
        self.create_triple_table()
        self.extract_properties()
        self.create_VP_tables()
        if(self.property_table_enabled):
            self.create_property_table()

def main():
    parser = argparse.ArgumentParser(description='Load a RDF into the Hive Mestastore using Vertical Partitioning.')
    parser.add_argument('input', metavar='input path', help='The HDFS path of the input RDF file')
    parser.add_argument('output', metavar='output database', help='The name of the database where to load the data. ')
    parser.add_argument('-s','-stats', metavar='[output stats file]', help='Statistics are computed and saved in the file.', default='')
    parser.add_argument('-p','-propertytable', metavar='[property table jar]', help='If loaded with property table, the jar of the separate loader as input.', default='')
    
    args = parser.parse_args()
    
    sc = SparkContext(appName="SparkVP_loader")
    sqlContext = HiveContext(sc)
   
    creator = VP_creator(sc, sqlContext, args.input, args.output, args.s)
    if(args.p):
        creator.set_propertytable_jar(args.p)
    creator.run_creator()

if __name__ == "__main__":
    main()
