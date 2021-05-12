# Covid Vaccination and related data etl.py
""" Scope the Project and Data decription

Scope:
What is the goal of this mini-project?

* The goal of this project is to gather data regarding the covid-19 cases, vaccination processes and demographics. We would like to create a DataLake that extracts the data from online sources and loads them into a database that can be used for analytics or any other data app.

What data do we use?

We use data from:
* OECD library regarding the demographics of the countries
* Dataworld for the covid-19 cases in all countries
* Vaccination processes from github sources

What is the end solution look like?

* It will be a etl.py script that will extract the covid data from the online sources and will create partition data in the data/outputs file directory

What tools did we use?

* Due to the big amount of data and the need of computationaly expensive queries, we used spark and aws.

"""


#Importing libraries

import pandas as pd
import io
import os

#for getting data from kaggle
import requests

#Spark related libraries
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import to_timestamp




def create_spark_session():
    """
        Function that creates and returns a spark session
    """
    spark = SparkSession.builder.appName("Vaccination_datalake").getOrCreate()

    return spark

def process_covid_cases_data(spark, input_data, output_data):
    """
        In this function we are loading the covid cases data and we partition the trasfromed data.
        Input: Sparksession, 
               Input_data filepath for covid cases data 
               Output_data filepath for covid cases data
               
        Output: We produce one parquet file. And return the pyspark dataframe for later usage.
    """

    # read cases data file
    df_cases = pd.read_csv(input_data)
    
    #quick preprocessing
    df_cases = df_cases[['report_date','continent_name','country_alpha_3_code','people_positive_new_cases_count','people_death_new_count']]
    
    #NaN values filled with uknown this is important for cleaning later on
    df_cases['country_alpha_3_code'] = df_cases['country_alpha_3_code'].fillna('unknown')
    df_cases['continent_name'] = df_cases['continent_name'].fillna('unknown')
    
    #Create spark dataframe
    df_spark_cases = spark.createDataFrame(df_cases)
    
    #Clear out the uknown
    col = 'continent_name'
    df_spark_cases = df_spark_cases.filter(df_spark_cases[col] != 'unknown')
    df_spark_cases.show(5)
    
    #trandform string date column to timestamp
    df_spark_cases = df_spark_cases.withColumn('report_date', to_timestamp(df_spark_cases.report_date, 'yyyy-MM-dd'))
    
    #Group by covid cases data by country and create covid cases table. (due to millions of lengths instances spark is very suitable for running this    action)
    covid_cases = df_spark_cases.groupBy("country_alpha_3_code")\
    .sum("people_positive_new_cases_count","people_death_new_count")
    
    #rename columns
    covid_cases = covid_cases.withColumnRenamed("country_alpha_3_code","country_code")
    covid_cases = covid_cases.withColumnRenamed("sum(people_positive_new_cases_count)","total_cases")
    covid_cases = covid_cases.withColumnRenamed("sum(people_death_new_count)","total_deaths")
    
    
    # Partition covid cases table
    covid_cases_par = covid_cases.write.partitionBy('country_code','total_cases').parquet(os.path.join(output_data, 'covid_cases.parquet'),'overwrite')
    print("covid_cases partitioned!")
    return df_spark_cases
    
def process_vaccines_and_countries(spark, input_data, output_data,df_spark_cases):
    #add url link to spark context
    spark.sparkContext.addFile(input_data)
    #load the url data into a csv form
    df_spark_vac = spark.read.csv("file://"+SparkFiles.get("vaccinations.csv"), header=True, inferSchema= True)
    
    #create table for vaccinations
    Vaccinations = df_spark_vac.select(
    df_spark_vac.iso_code.alias('country_code'),   
    df_spark_vac.date.alias('date_entry'),
    df_spark_vac.people_vaccinated,
    df_spark_vac.total_vaccinations,
    df_spark_vac.people_fully_vaccinated,
    df_spark_vac.daily_vaccinations,
    df_spark_vac.daily_vaccinations_per_million)
    
    # Partition Vaccinations table
    Vaccinations_par = Vaccinations.write.partitionBy('country_code').parquet(os.path.join(output_data, 'Vaccinations.parquet'), 'overwrite')
    print("Vaccinations partitioned!")
    
    #create country data from df spark cases
    countries = df_spark_cases.select('continent_name', "country_alpha_3_code").distinct()

    # add country full name
    countries = countries.join(df_spark_vac,df_spark_vac.iso_code == countries.country_alpha_3_code)\
    .select(countries.country_alpha_3_code.alias('country_code'),
        countries.continent_name,
        df_spark_vac.location.alias('country_name'))
    
    # Partition country table
    countries_par = countries.write.partitionBy('country_code','continent_name').parquet(os.path.join(output_data, 'countries.parquet'), 'overwrite')
    print("countries partitioned!")
    
def process_GDPR_data(spark, input_data, output_data):
    """
        In this function we are loading the gdpr file that is saved localy and create a table.
        Input: Sparksession, 
               Input_data filepath for gdpr data 
               Output_data filepath for gdpr data
               
        Output: We produce a parquet filea for the gdpr table.
    """

    # read song data file
    df_spark_GDPR =spark.read.csv(input_data, 
    header=True, 
    inferSchema= True)
    
    #Create table for partition
    GDPR_info = df_spark_GDPR.select(
    df_spark_GDPR.LOCATION.alias('country_code'),   
    df_spark_GDPR.INDICATOR,
    df_spark_GDPR.SUBJECT,
    df_spark_GDPR.FREQUENCY,
    df_spark_GDPR.TIME,
    df_spark_GDPR.Value,
    )
    
    #partition (size for this case is small but in our pipeline we would like to keep it in case the future this data increases)
    GDPR_info_par = GDPR_info.write.partitionBy('country_code','TIME').parquet(os.path.join(output_data, 'GDPR.parquet'), 'overwrite')
    print("GDPR_info partitioned!")

def query_checks(spark,output_data):
    """
        In this function we are loading data from our datalake and use queries for quality check
        Input: Sparksession, 
               Output_data filepath for parquet files
               
        Output: We produce two queries for the quality check
    """
    
    #read parquet files
    Vaccinations = spark.read.parquet(os.path.join(output_data, 'Vaccinations.parquet'))
    
    countries = spark.read.parquet(os.path.join(output_data, 'countries.parquet'))
    
    covid_cases = spark.read.parquet(os.path.join(output_data, 'covid_cases.parquet'))
    
    #query 1
    #Current amount of vaccinations in Greece
    Vaccinations.join(countries,countries.country_code == Vaccinations.country_code)\
    .select(countries.country_name,Vaccinations.people_vaccinated,Vaccinations.people_fully_vaccinated).groupBy('country_name')\
    .sum('people_vaccinated','people_fully_vaccinated').where(countries.country_name == 'Greece').show()
    
    #query 2
    # Current amount of cases in Greece
    covid_cases.join(countries,countries.country_code == covid_cases.country_code)\
    .select(countries.country_name,covid_cases.total_cases,covid_cases.total_deaths).where(countries.country_name == 'Greece').show(1)

def main():
    #define our spark session
    spark = create_spark_session()
    
    #Input Sources
    covid_cases_input = "https://download.data.world/s/s65p7s2aqym4qams7ub2n4b72e5e4h"
    gdpr_dir = 'Health spendings per country GDPR percentage.csv'
    url_vac = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv"
    
    #Output partition data directory
    output_data = 'data/outputs/vaccine_data'
    
    print('Starting to load')
    #process covid cases data (and save the dataframe for later use)
    df_spark_cases = process_covid_cases_data(spark, covid_cases_input, output_data)
    
     
    #process GDPR data from local tables
    process_GDPR_data(spark, gdpr_dir, output_data)
    
    
    #process vaccination and country data
    process_vaccines_and_countries(spark, url_vac, output_data,df_spark_cases)
    del df_spark_cases
    
    #run quality checks queries
    query_checks(spark,output_data)
    print("finished with quality checks succesfully!")

if __name__ == "__main__":
    main()