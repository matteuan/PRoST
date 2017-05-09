import sys, re
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
        sqlContext.sql("USE " + self.outputDB)

    def create_triple_table(self):
        triple_table = self.sqlContext.sql(
            "CREATE EXTERNAL TABLE tripletable (s STRING, p STRING, o STRING) ROW FORMAT DELIMITED" \
            + " FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' LOCATION '"+ self.inputFile +"'")
        
    def extract_properties(self):
        # we assume that the number of properties is small
	    for p in self.sqlContext.sql('SELECT DISTINCT * FROM tripletable').collect():
		    self.properties[p["p"]] = True

    def create_VP_tables(self):
        print "There are %(num)d properties" % {"num": len(self.properties)}
        # for each distinct property, create a table
        for p in self.properties:
            prop_df = self.sqlContext.sql("SELECT s AS s, o AS o FROM tripletable WHERE p='" + p + "'")
            df_writer = DataFrameWriter(prop_df)
            df_writer.saveAsTable("VP_" + valid_string(p))
    
    def run_creator(self):
        self.create_triple_table()
        self.extract_properties()
        self.create_VP_tables()


""" 
TODO: use the CLI options
TODO: print or log statistics (e.g. number of properties created)
"""
def main():
    sc = SparkContext(appName="GYM_loader")
    sqlContext = HiveContext(sc)
    if len(sys.argv) < 3:
        print "Usage:", sys.argv[0], "[input file] [output database]"
        sys.exit()
    inp_file = sys.argv[1]
    out_db = sys.argv[2]
    creator = VP_creator(sc, sqlContext, inp_file, out_db)
    creator.run_creator()

if __name__ == "__main__":
    main()
