import configparser
import os
from pyspark.sql import SparkSession

import pandas as pd
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re 



config = configparser.ConfigParser()
config.read('dwh.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('USER','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('USER','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_covid_data(spark, output_path):
    '''
    Extract the covid data from external source & Clean up the data & Save the data in S3 defined destination

    '''    
    
    # Store the url as a string scalar: url => str
    url = "https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports"
    
    # Issue request: r => requests.models.Response
    r = requests.get(url)

    # Extract text: html_doc => str
    html_doc = r.text

    # Parse the HTML: soup => bs4.BeautifulSoup
    soup = BeautifulSoup(html_doc, "lxml")

    # Find all 'a' tags (which define hyperlinks): a_tags => bs4.element.ResultSet
    a_tags = soup.find_all('a')

    # Store a list of urls ending in .csv: urls => list
    urls = ['https://raw.githubusercontent.com'+re.sub('/blob', '', link.get('href')) 
            for link in a_tags  if '.csv' in link.get('href')]

    # Initialise an empty list the same length as the urls list: df_list => list
    df_list = [pd.DataFrame([None]) for i in range(len(urls))]

    # Store an empty list of dataframes: df_list => list
    df_list = [pd.read_csv(url, sep = ',') for url in urls]

    # load all csv files into one dataframe
    df= pd.concat(df_list, axis=0, ignore_index=True, sort=False)
    
    # Clean up Covid-19 Daily Report Data

    #one type of colunm names
    col_A=["FIPS","Admin2","Province_State","Country_Region","Last_Update","Lat","Long_","Confirmed","Deaths","Recovered"]
    #the other type of column names
    col_B=["FIPS","Admin2","Province/State","Country/Region","Last Update","Latitude","Longitude","Confirmed","Deaths","Recovered"]

    df_A=df[df['Country/Region'].isna()][col_A].copy()
    df_A.drop_duplicates(inplace=True)
    df_A.rename(columns = {'Lat':'Latitude', 'Long_':'Longitude','Province_State':'State','Country_Region':'Country'},inplace=True)
    df_A["Last_Update"]=pd.to_datetime(df_A.Last_Update)
    df_A["Date"]=df_A["Last_Update"].dt.date


    df_B=df[~df['Country/Region'].isna()][col_B].copy()
    df_B.drop_duplicates(inplace=True)
    df_B.rename(columns={'Province/State':'State', 'Country/Region':'Country','Last Update':'Last_Update'}, inplace=True)
    df_B["Last_Update"]=pd.to_datetime(df_B.Last_Update)
    df_B["Date"]=df_B["Last_Update"].dt.date

    df_covid=df_A.append(df_B)
    #Clean up the column format
    df_covid['FIPS']=df_covid['FIPS'].astype('float64')
    df_covid['Latitude']=df_covid['Latitude'].astype('float64')
    df_covid['Longitude']=df_covid['Longitude'].astype('float64')
    df_covid['Confirmed']=df_covid['Confirmed'].astype('float64')
    df_covid['Deaths']=df_covid['Deaths'].astype('float64')
    df_covid['Recovered']=df_covid['Recovered'].astype('float64')
    df_covid['Year']=df_covid["Last_Update"].dt.year
    df_covid['Month']=df_covid["Last_Update"].dt.month

    #this step is used to create spark dataframe so that it will not cause any field type conflict
    df_covid['Admin2'].fillna("None", inplace=True)
    df_covid['State'].fillna("None", inplace=True)

    df_covid.drop_duplicates(inplace=True)
    df_covid.sort_values(by=["Year","Month","Country","State"], inplace=True)
    df_covid.reset_index(drop=True, inplace=True)
    
    #Save the Daily Covid data
    sdf_covid=spark.createDataFrame(df_covid)
    sdf_covid.write.partitionBy("year","month").mode("overwrite").parquet(output_path)
    

# map the state to align with Covid-19 data
# Cleaned up majority of the countries except Russia
def map_state(x):
    #US state update
    if x=="Georgia (State)":
        return "Georgia"
    #China state update
    elif  x=="Ningxia Hui":
        return "Ningxia"
    elif x=="Xinjiang Uygur":
        return "Xinjiang"
    elif x=="Xizang":
        return "Tibet" 
    elif x=="Nei Mongol":
        return "Inner Mongolia"
    #India state update
    elif x=="Andaman And Nicobar":
        return "Andaman and Nicobar Islands"
    elif x=="Dadra And Nagar Haveli":
        return "Dadra and Nagar Haveli and Daman and Diu"
    elif x=="Daman And Diu":
        return "Dadra and Nagar Haveli and Daman and Diu"
    elif x=="Orissa":
        return "Odisha"
    #Russia state update
    elif x=="Adygey":
        return "Adygea Republic"
    elif x=="Amur":
        return "Amur Oblast"
    elif x=="Altay":
        return "Altai Republic"
    elif x=="Arkhangel'Sk":
        return "Arkhangelsk Oblast"
    elif x=="Astrakhan'":
        return "Astrakhan Oblast"
    elif x=="Bashkortostan":
        return "Bashkortostan Republic"
    elif x=="Belgorod":
        return "Belgorod Oblast"
    elif x=="Bryansk":
        return "Bryansk Oblast"
    elif x=="Buryat":
        return "Buryatia Republic"
    elif x=="Chechnya":
        return "Chechen Republic"
    elif x=="Chelyabinsk":
        return "Chelyabinsk Oblast"
    else:
        return x
    
def process_temp_data(spark, output_path):
    #Read in Global Land Temperature by State csv (downloaded manually in local directory) as dataframe
    df_temp=pd.read_csv("GlobalLandTemperaturesByState.csv")
   
    #Clean up the data
    df_temp['Country']=df_temp['Country'].apply(lambda x: 'US' if x == 'United States' else x)
    df_temp['State']=df_temp['State'].apply(lambda x: map_state(x))
    
    # Save the temperature data
    sdf_temp = spark.createDataFrame(df_temp_grp)
    sdf_temp.write.mode("overwrite").parquet(output_path)


#Clean up Indian population data
def map_india_state(x):   
    if x=="Andaman & Nicobar Islands":
        return "Andaman and Nicobar Islands"
    elif x=="Dadra & Nagar Haveli":
        return "Dadra and Nagar Haveli and Daman and Diu"
    elif x=="Daman & Diu":
        return "Dadra and Nagar Haveli and Daman and Diu"
    elif x=="Jammu & Kashmir":
        return "Jammu and Kashmir"
    elif x=="Odisha (Orissa)":
        return "Odisha"
    elif x=="Puducherry (Pondicherry)":
        return "Puducherry"
    else:
        return x    
    
def process_pop_data(spark, output_path):
    #Read in popluation data csvs (downloaded manually in local directory) as dataframe
    df_china_base=pd.read_csv("china_provinces_population.csv")
    df_us_base=pd.read_csv("sc-est2020-18+pop-res.csv")
    df_india_base=df_india_base=pd.read_csv('district wise population for year 2001 and 2011.csv')
   
    #Clean up China population data
    df_china=df_china_base.copy()
    df_china['Country']='China'
    df_china.rename(columns={'PROVINCE NAME':'State', 'POPULATION':'Population'}, inplace=True)

    #Clean up US population data
    df_us=df_us_base[df_us_base['NAME'] !='United States'][['NAME', 'POPESTIMATE2020']]
    df_us['Country']='US'
    df_us.rename(columns={'NAME':'State', 'POPESTIMATE2020':'Population'}, inplace=True)
    
    #Clean up India population data
    df_india_base['State']=df_india_base['State'].apply(lambda x: map_india_state(x))
    df_india=df_india_base[['State','Population in 2011']].groupby(['State'])['Population in 2011'].sum().to_frame().reset_index()
    df_india['Country']='India'
    df_india.rename(columns={'Population in 2011':'Population'}, inplace=True)

   
    #Combine population data from all three countries 
    df_population= pd.concat([df_us,df_india, df_china], axis=0, ignore_index=True, sort=False)
    
    # Save the population data
    sdf_pop = spark.createDataFrame(df_population)
    sdf_pop.write.mode("overwrite").parquet(output_path)

def main():
    spark = create_spark_session()
    
    covid_data_path="s3a://udacity-capston/DailyCovid"
    temp_data_path="s3a://udacity-capston/AvgTemperatureByState"
    pop_data_path="s3a://udacity-capston/PopulationByState"
    
    process_covid_data(spark, covid_data_path)    
    process_temp_data(spark, temp_data_path)
    process_pop_data(spark, pop_data_path)
    

if __name__ == "__main__":
    main()
