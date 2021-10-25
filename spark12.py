from pyspark import SparkContext, SQLContext

if __name__ == '__main__':
      # Definition of common variables
    filename="file:///home/puneet/Documents/documents/ContAssessment3/DataCoSupplyChainDataset.csv"
    file = open("/home/puneet/Documents/documents/ContAssessment3/results.txt",'w')

	# Define SparkContext and SQLContext
sc = SparkContext(appName = "Test3")
sqlCtx = SQLContext(sc)