import pyspark
from pyspark.sql.session import SparkSession

from pyspark.sql import SQLContext
from pyspark import SparkFiles
from pyspark.sql.functions import concat, col, lit
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql import Window
from pyspark.sql.functions import col
import pyspark.sql.functions as F

#reading in/adjusting the 3 data frames from github

#Here is how I read in the files, its a bit tedious but not too bad. Hopefully this works for you two as well. 

#Airports
url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
sc.addFile(url)
sqlContext = SQLContext(sc)

df = sqlContext.read.csv(SparkFiles.get("airports.dat"), header=False, inferSchema= True) 

new_col_names = 'ID', 'Name', 'City', 'Country','IATA', 'ICAO', 'Lat', 'Long', 'Alt', 'Timezone', 'DST', 'Tz database time zone', 'type', 'source'
df = df.toDF(*new_col_names)


#Routes
url2 = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"
sc.addFile(url2)
sqlContext2 = SQLContext(sc)

df2 = sqlContext2.read.csv(SparkFiles.get("routes.dat"), header=False, inferSchema= True) 

new_col_names2 = 'Airline', 'Airline ID', 'Source Airport', 'Source Airport ID', 'Dest Airport', 'Dest Airport ID', 'Codeshare', 'Stops', 'equipment'
df2 = df2.toDF(*new_col_names2)


#Airlines
url3 = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat"
sc.addFile(url3)
sqlContext3 = SQLContext(sc)

df3 = sqlContext3.read.csv(SparkFiles.get("airlines.dat"), header=False, inferSchema= True) 

new_col_names3 = "Airline ID", "Airline Name", "Alias", "IATA", "ICAO", "Callsign", "Country", "Active (Y/N)"
df3 = df3.toDF(*new_col_names3)


############################################################Find Flight Algorithm#################################################################################
from pyspark.sql.functions import col
import pyspark.sql.functions as F

def find_flight():
    source = input("Enter Your Source City: ")
    dest = input("Enter Your Destination City: ")

    
    source_airports = df.filter(col("City") == source)
    dest_airports = df.filter(col("City") == dest)
    
    source_codes = list(source_airports.select('IATA').toPandas()['IATA'])
    dest_codes = list(dest_airports.select('IATA').toPandas()['IATA'])
    
    flights = df2.filter(F.col("Source Airport").isin(source_codes))
    flights2 = flights.filter(F.col("Dest Airport").isin(dest_codes))
    
    airline_codes = list(flights2.select('Airline').toPandas()['Airline'])
    airlines = df3.filter(F.col("IATA").isin(airline_codes))
    
    source_codes2 = list(flights2.select('Source Airport').toPandas()['Source Airport'])
    dest_codes2 = list(flights2.select('Dest Airport').toPandas()['Dest Airport'])
    
    #For source names
    source_names = list(source_airports.select('Name').toPandas()['Name'])
    source_codes = list(source_airports.select('IATA').toPandas()['IATA'])
    sourcelist = [list(a) for a in zip(source_names, source_codes)]
    source = list()
    
    for i in range(len(source_codes2)):
        for j in range(len(sourcelist)):
            if source_codes2[i] == sourcelist[j][1]:
                source.append(sourcelist[j][0])
    
    #For setination names    
    dest_names = list(dest_airports.select('Name').toPandas()['Name'])
    dest_codes = list(dest_airports.select('IATA').toPandas()['IATA'])
    destlist = [list(a) for a in zip(dest_names, dest_codes)]
    dest = list()
    for i in range(len(dest_codes2)):
        for j in range(len(destlist)):
            if dest_codes2[i] == destlist[j][1]:
                dest.append(destlist[j][0])   
    #Lists to use for printing flight values 
    airline_names = list(airlines.select('Airline Name').toPandas()["Airline Name"])
    stops = list(flights2.select("Stops").toPandas()["Stops"])
    for h, i, j, k in zip(source, dest, airline_names, stops):
        print("Flight from", h, "to", i, "on", j, "with", k, "stops")
    

    
find_flight()
###################################################################find_popular_airports####################################################################################


def find_popular_airports():
    amount = input("How many airports would you like to see ranked: ")
    df3 = df2.groupBy('Source Airport').count()
    a = df3.sort(col('count').desc())
    df4 = df2.groupBy('Dest Airport').count()
    departures = df4.sort(col('count').desc())
    departures = list(departures.select('count').toPandas()['count'])
    b = sqlContext.createDataFrame([(l,) for l in departures], ['Departures'])
    a = a.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
    b = b.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
    final_df = a.join(b, a.row_idx == b.row_idx).\
             drop("row_idx")
    df1=final_df.withColumn("sum", col("count")+col("Departures"))

    cities = list(df1.select('Source Airport').toPandas()['Source Airport'])
    flights = list(df1.select('sum').toPandas()['sum'])
    
    cities = cities[0:int(amount)]
    flights = flights[0:int(amount)]
    print("Most Popular Airports by Flight: ")
    for i, j in zip(cities, flights):
        print(i,":",j)
    ##############################################################################################################################################################
#prints info on airports in given city
def print_info():
    city= input("Enter City to output airport(s) info: ")
    
    airports = df.filter(col("City") == city)
    codes = list(airports.select('IATA').toPandas()['IATA'])
    names = list(airports.select('Name').toPandas()['Name'])
    country = list(airports.select('Country').toPandas()['Country'])
    timezone = list(airports.select('Timezone').toPandas()['Timezone'])
    lat = list(airports.select('Lat').toPandas()['Lat'])
    long = list(airports.select('Long').toPandas()['Long'])
    
    for x in range(len(airports)):
        print("\n", names[x])
        print("\nCity:", airports[x])
        print("\nCountry:", country[x])
        print("\nIATA:", codes[x])
        print("\nTimezone:", timezone[x])
        print("\nLatitude:", lat[x])
        print("\nLongitude:", long[x])