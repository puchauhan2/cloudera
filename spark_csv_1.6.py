'''
echo 'export JAVA_TOOL_OPTIONS="-Dhttps.protocols=TLSv1.2"' >> ~/.bashrc
source ~/.bashrc


pyspark --packages com.databricks:spark-csv_2.10:1.5.0

'''

from pyspark import SparkContext, HiveContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.functions import rank
from pyspark import SparkConf

if __name__ == '__main__':
    
	# Definition of common variables
    #filename="file:///home/puneet/Documents/documents/ContAssessment3/DataCoSupplyChainDataset.csv"
	filename="file:///home/cloudera/Desktop/Puneet/DataCoSupplyChainDataset.csv"
    #file = open("/home/puneet/Documents/documents/ContAssessment3/results.txt",'w')
	file = open("/home/cloudera/Desktop/Puneet/results.txt",'w')

    # Define SparkContext and SQLContext
	
	conf = SparkConf()
	conf.setAppName("Bigdata Practice")
	sc = SparkContext.getOrCreate(conf=conf)
	sqlContext = HiveContext(sc)
	
	###Q1 - Load data, convert to dataframe, apply appropriate column names and variable types

	# Your solution goes here
    schema = StructType() \
          .add("Type",StringType(),True) \
          .add("Customer_Country",StringType(),True) \
          .add("Customer_Fname",StringType(),True) \
          .add("Customer_Id",IntegerType(),True) \
          .add("Customer_Segment",StringType(),True) \
          .add("Order_Item_Product_Price",DoubleType(),True) \
          .add("Order_Item_Quantity",IntegerType(),True) \
          .add("Order_Item_Total",DoubleType(),True) \
          .add("Product_Name",StringType(),True) 
		  
	df = sqlContext.read.format("csv").option("header", False).schema(schema).load(filename)


    df.show()

	###Q2 - Determine the proportion of each customer segment

	# Your solution goes here
    total_number_row = df.count()
    consumer_count = str((df.where(df.Customer_Segment == 'Consumer').count()*100)/total_number_row)
    corporate_count = str((df.where(df.Customer_Segment == 'Corporate').count()*100)/total_number_row)
    home_office_count = str((df.where(df.Customer_Segment == 'Home Office').count()*100)/total_number_row)

    print("The proportion of each customer segment are: consumer = " +  consumer_count + " %, Corporate = " + corporate_count + " %, Home Office = " + home_office_count + " %\n\n" )

      # Printing the solution to the results.txt file
    file.write("The proportion of each customer segment are: consumer = " +  consumer_count + " %, Corporate = " + corporate_count + " %, Home Office = " + home_office_count + " %\n\n" )

	###Q3 - Which three products had the most sales

	# Your solution goes here
    dff = df.groupBy("Product_Name").sum("Order_Item_Total")
    dff2 = dff.orderBy('sum(Order_Item_Total)', ascending=False)
    dxt = dff2.limit(3)
    dxt.show()

      # Printing the solution to the results.txt file
    print( "Top 3 total item sales in the data set are: " + str(dxt.collect()[0][0]) + " = $" + str(dxt.collect()[0][1]) + ", " + str(dxt.collect()[1][0]) + " = $" + str(dxt.collect()[1][1]) + ", " + str(dxt.collect()[2][0]) + " = $" + str(dxt.collect()[2][1]) + "\n\n")
    file.write("Top 3 total item sales in the data set are: " +  str(dxt.collect()[0][0]) + " = $" + str(dxt.collect()[0][1]) + ", " + str(dxt.collect()[1][0]) + " = $" + str(dxt.collect()[1][1]) + ", " + str(dxt.collect()[2][0]) + " = $" + str(dxt.collect()[2][1]) + "\n\n")

	###Q4 - For each transaction type, determine the average item cost before discount

	# Your solution goes here
    avgtrans2 = df.groupBy("Type").agg(F.mean(df['Order_Item_Product_Price']*df['Order_Item_Quantity'])).collect()
    file.write("Question 4: The average of each item cost per transaction type are: %s = $ " %avgtrans2[0][0] + "%s," %avgtrans2[0][1] + "%s = $ " %avgtrans2[1][0] + "%s," %avgtrans2[1][1] + "%s = $ " %avgtrans2[2][0] + "%s," %avgtrans2[2][1] + "%s = $ " %avgtrans2[3][0] + "%s," %avgtrans2[3][1] +"\n\n")
	
      ###Q5 - What is the most regular customer first name in Puerto Rico

	# Your solution goes here
    sdf=df.groupBy("Customer_Country","Customer_Fname").count()
    windowSpec  = Window.partitionBy("Customer_Country").orderBy(col("count").desc())
    dfs = sdf.withColumn("rank",rank().over(windowSpec))
    dfr=dfs.filter(col("rank")== 1)
    puerto_users=dfr.filter(col("Customer_Country")== 'Puerto Rico')
    puerto_users.show()

    # Printing the solution to the results.txt file
    print("The Most regular customer name in Puerto Rico is " + str(puerto_users.collect()[0][1]) + ", who comes back " + str(puerto_users.collect()[0][2]) + " times.")
    file.write("The Most regular customer name in Puerto Rico is " + str(puerto_users.collect()[0][1]) + ", who comes back " + str(puerto_users.collect()[0][2]) + " times.")
    file.close()

