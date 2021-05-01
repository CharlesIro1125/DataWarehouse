import os
from datetime import timedelta
from datetime import datetime
import pandas as pd
from pyspark.sql.functions import col,split,expr,udf,trim,mean,sum,create_map,explode,year
import time
from variable2 import *
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import types as T
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType,MapType
from pyspark.sql.functions import expr
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import re
import configparser


config = configparser.ConfigParser()
config.read_file(open(os.path.join(os.getcwd(),'dl.cfg')))

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Description: 
        
            This function crestes a spark session with a hadoop
            package/utility and returns the instance of the spark session.
        
        Returns:
            spark : spark seessin
    """
    
    
    print('initialising spark session')
    
    spark = SparkSession.builder.\
            config('spark.jars.packages','org.apache.hadoop:hadoop-aws:2.7.0').\
            enableHiveSupport().getOrCreate()

    
    return spark


def processing_immigration_data(spark, output_data):
    """
        Description:       
            This function reads the immigration file using the
            spark read method and performs data cleaning, processing,
            and finally saves the cleaned data as parquet file to S3.
            
        Arguments:  
            spark : spark session.
            output_data: the output root directory to the s3 bucket.
               
        Returns:
            None.
        
    """
    df_immigration = spark.read.csv('immigrationData.csv',schema=paramSchema,header=True)   
    
    df_immigration = df_immigration.na.fill({'i94mode':0,'i94addr':'Nil','i94bir':0,'dtadfile':'Nil',
                'visapost':'Nil','occup':'Nil','entdepa':'Nil','entdepd':'Nil','entdepu':'Nil','matflag':'Nil',
                'biryear':0,'dtaddto':'Nil','gender':'Nil','insnum':'Nil','airline':'Nil','fltno':'Nil'})
        
    
    replaceCountry = udf( lambda x:  country_code[x])

    replaceVisa  = udf(lambda x: visa_code[x])
    
    replaceMode = udf(lambda x: mode_of_entry_code[x])
    
    #replaceAddress = udf(lambda x: address_code[x])
    
    replacePort = udf(lambda x: port_of_entry_code[x])
    
    get_date = udf(lambda x: (datetime(1960, 1, 1).date() + timedelta(x)).isoformat() if x else None )
    
    
    df_immigration = df_immigration.withColumn('i94citizenship',replaceCountry(df_immigration['i94cit'])).drop("i94cit").\
                withColumn('i94resident',replaceCountry(df_immigration['i94res'])).drop("i94res","_c0").\
                withColumn('Visa',replaceVisa(df_immigration['i94visa'])).drop("i94visa").\
                withColumn('i94PortOfEntry',replacePort(df_immigration['i94port'])).drop("i94port").\
                withColumn('i94modeOfEntry',replaceMode(df_immigration['i94mode'])).drop("i94mode").\
                withColumn('i94AddressTo',trim(split("i94addr"," ")[0])).drop("i94addr")
    
    df_immigration = df_immigration.withColumn('i94PortOfEntryCity',F.lower(trim(split("i94PortOfEntry",",")[0]))).\
                    withColumn('i94Port',F.upper(trim(split("i94PortOfEntry",",")[1]))).\
                    withColumn('i94PortOfEntryState',split("i94Port"," ")[0]).drop('i94Port').\
                    withColumn('arrdate',get_date("arrdate")).drop("i94PortOfEntry").\
                    withColumn('arrivedate',col("arrdate").cast('date')).drop("i94PortOfEntry",'arrdate').\
                    withColumn('ddate',get_date("depdate")).drop("arrdate","depdate").\
                    withColumn('departdate',col("ddate").cast('date')).drop("arrdate","depdate",'ddate').\
                    withColumn('i94year',col('i94yr').cast('integer')).\
                    withColumn('i94month',col('i94mon').cast('integer')).drop("i94yr","i94mon").\
                    withColumn('i94age',col('i94bir').cast('integer')).drop("i94bir").\
                    withColumn('i94birthyear',col('biryear').cast('integer')).drop("biryear","count")
                    
        
    df_immigration = df_immigration.where(df_immigration.i94modeOfEntry == 'Air').where(df_immigration.i94age >= 0)
    
    df_immigration = df_immigration.na.fill({'i94PortOfEntryState':'Nil'})
    
    df_immigration = df_immigration.dropDuplicates(['cicid'])
    
    df_immigration = df_immigration.select('cicid','i94citizenship','i94resident','Visa','i94modeOfEntry',
            'i94AddressTo','i94PortOfEntryCity','i94PortOfEntryState','gender','arrivedate','departdate',
            'i94year','i94month','i94birthyear','i94age','fltno','airline','visatype','visapost','dtadfile',
            'occup','entdepa','entdepd','entdepu','matflag','dtaddto','insnum','admnum')
    
    #try:
    print("writing df_immigration table to s3 bucket")
    df_immigration.write.mode("overwrite").partitionBy("i94year","i94month").\
        parquet(os.path.join(output_data,"immigrationData"))
       # print("writing Song table completed")
    #except:
        #print('unable to write Song table')


  
    
    

def processing_airport_data(spark, output_data):
    """
        Description: 
        
            This function reads the airport code file using the
            spark read method and performs data cleaning, processing,
            and finally saves the cleaned data as parquet file to S3.
              
        Arguments:
        
            spark: spark session.
            output_data: the output root directory to the s3 bucket.
           
        Returns:
            None

    """
    airportCode_df = spark.read.csv("Airportgeocode.csv",sep = ',',header = True)
    
    airportCode_df = airportCode_df.withColumn('iso_region_state',F.upper(split("iso_region","-")[1])).drop("iso_region").\
            withColumn('ElevationFt',col("elevation_ft").cast('integer')).drop("elevation_ft","_c0")
    
    airportCode_df = airportCode_df.na.fill({'ElevationFt':0,'municipality':'Nil','gps_code':'Nil','iata_code':'Nil',
                        'local_code':'Nil'})
    
    airportCode_df = airportCode_df.dropDuplicates(['ident'])
    
    @udf
    def extractCity(line):
        import re
        x = re.search(r"('city':\s*('*\s*\w*(\s*\w*)*\s*'))",line) 
        if x:
            x = x.group(2)
            val = x.replace("'","").strip()
        else:
            val = 'Nil'
        return val
    
    airportCode_df = airportCode_df.withColumn("City",extractCity('geocode'))
    
    airportCode_df = airportCode_df.dropDuplicates(['City'])
    
    
    
    print("writing airportCode_df table to s3 bucket")
    airportCode_df.write.mode("overwrite").parquet(os.path.join(output_data,"airportData"))
    print("writing airportCode_df table completed")

        
        
        
        
def processing_demography_data(spark, output_data):
    
    """
       Description: 
            
            This function reads in the population demography file using the
            spark read method and performs data cleaning, processing,
            and finally saves the cleaned data as parquet file to S3.
              
            
        Arguments:
        
            spark: spark session
            output_data: the output root directory to the s3 bucket
           
        Returns:
            None     
    """
    us_demogr_df = spark.read.csv("us-cities-demographics.csv", sep=";",schema=paramSchema1, header=True)
   
    us_demogr_df = us_demogr_df.select([(F.lower(us_demogr_df.City)).alias('City'),
                    (F.lower(us_demogr_df.State)).alias('State'),
                    (us_demogr_df['Median Age']).alias('MedianAge'),
                    (us_demogr_df['Male Population']).alias('MalePopulation'),
                    (us_demogr_df['Female Population']).alias('FemalePopulation'),
                    (us_demogr_df['Total Population']).alias('TotalPopulation'),
                    (us_demogr_df['Number of Veterans']).alias('NumberOfVeterans'),
                    (us_demogr_df['Foreign-born']).alias('ForeignBorn'),
                    (us_demogr_df['Average Household Size']).alias('AverageHouseholdSize'),
                    (F.upper(us_demogr_df['State Code'])).alias('StateCode'),'Race'])
    
    cities_demogr_dropna = us_demogr_df.dropna(how ='any')
    
    mean_values = cities_demogr_dropna.select([F.mean('MalePopulation'),F.mean('FemalePopulation'),\
                F.mean('NumberOfVeterans'),F.mean('ForeignBorn'),F.mean('AverageHouseholdSize')]).toPandas()
    
    us_demogr_df = us_demogr_df.na.fill({'MalePopulation': mean_values['avg(MalePopulation)'].tolist()[0],\
                             'FemalePopulation':mean_values['avg(FemalePopulation)'].tolist()[0],\
                             'NumberOfVeterans': mean_values['avg(NumberOfVeterans)'].tolist()[0],\
                             'ForeignBorn': mean_values['avg(ForeignBorn)'].tolist()[0],\
                             'AverageHouseholdSize': mean_values['avg(AverageHouseholdSize)'].tolist()[0]})
    
    
    us_state_demogr = us_demogr_df.groupBy('StateCode','State')\
        .agg({'MedianAge':'mean','MalePopulation':'sum','FemalePopulation':'sum','TotalPopulation':'sum',
             'NumberOfVeterans':'sum','ForeignBorn':'sum','AverageHouseholdSize':'mean'})
    
    for column in us_state_demogr.columns:
        start_index = column.find('(')
        end_index = column.find(')')
        if (start_index and end_index) != -1:
            us_state_demogr = us_state_demogr.withColumnRenamed(column, column[start_index+1:end_index])
        else:
            continue
            
    
    #try:
    print("writing us_state_demogr table to s3 bucket")
    us_state_demogr.write.mode("overwrite").parquet(os.path.join(output_data,"demographyData"))
        #print("writing us_state_demogr table completed")
    #except:
        #print('unable to write us_state_demogr table')

    
    
def processing_temp_data(spark, output_data): 
    
    """
       Description: 
            
            This function reads in the land surface temperature file using the
            spark read method and performs data cleaning, processing,
            and finally saves the cleaned data as parquet file to S3.
              
            
        Arguments:
        
            spark: spark session
            output_data: the output root directory to the s3 bucket
           
        Returns:
            None     
    """
    
    landtemp_df = spark.read.csv("GlobalLandTemperaturesByState.csv",schema=paramSchema2,header=True)
    
    landtemp_df = landtemp_df.where(landtemp_df['Country'] == 'United States')
    
    landtemp_df = landtemp_df.withColumn('statelowercase',F.lower(landtemp_df.State))\
                            .withColumn('year',year(landtemp_df.dt)).drop('State')
                            
    landtemp_df = landtemp_df.withColumnRenamed('statelowercase','state')
    
    landtemp_df = landtemp_df.na.drop(subset=['AverageTemperature'])
    
    landtemp_df = landtemp_df.withColumn('id',F.monotonically_increasing_id())
    
    landtemp_df = landtemp_df.select('id','dt','AverageTemperature','AverageTemperatureUncertainty',
                                   'Country','state','year')
    
    try:
        print("writing landtemp_df table to s3 bucket")
        landtemp_df.write.mode("overwrite").partitionBy("year").parquet(os.path.join(output_data,"landtempData"))
        print("writing landtemp_df table completed")
    except:
        print('unable to write landtemp_df table')

    
  
    
def main():
    
    """
    Description:
        The main method initialises some variables
        and executes the various defined methods
    
    """
    
    output_data = "s3a://charlesproject-sink/"
    spark = create_spark_session()
    
    processing_immigration_data(spark, output_data)
    processing_airport_data(spark, output_data)
    processing_demography_data(spark, output_data)
    processing_temp_data(spark, output_data)
        

if __name__ == "__main__":
    main()

