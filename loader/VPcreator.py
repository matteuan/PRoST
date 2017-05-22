import sys, re, argparse
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import DataFrameWriter
import pyspark.sql.functions as f


"""
valid_string converts in '_' all the characters that are not
accepted as table names in the hive metastore. 
"""
def valid_string(s):
    return re.sub(r"[^\w]", "_", s)

#TODO: errors handling
class VP_creator:
    """ 
    VP_creator creates for an input RDF dataset
    the corresponding Triple Table, Vertical Partitioning tables (VP)
    """

    def __init__(self, sc, sqlContext, inputFile, outputDB):
        self.sc = sc
        self.sqlContext = sqlContext
        self.inputFile = inputFile
        self.outputDB = outputDB
        self.properties = {}
        
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
        print "Tables created: %d / %d "  % (i, total_properties)

    def run_creator(self):
        self.create_triple_table()
        self.extract_properties()
        self.create_VP_tables()

def main():
    parser = argparse.ArgumentParser(description='Load a RDF into the Hive Mestastore using Vertical Partitioning.')
    parser.add_argument('input', metavar='[input file]', help='The HDFS path of the input RDF file.')
    parser.add_argument('output', metavar='[output database]', help='The name of the database where to load the data. ')
    args = parser.parse_args()

    sc = SparkContext(appName="GYM_loader")
    sqlContext = HiveContext(sc)
   
    creator = VP_creator(sc, sqlContext, args.input, args.output)
    creator.run_creator()

if __name__ == "__main__":
    main()
