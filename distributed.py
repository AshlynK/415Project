import pyspark
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkFiles
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.functions import concat, col, lit
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql import Window

#reading in/adjusting the 3 data frames from github

#Airports
url = "https://raw.githubusercontent.com/AshlynK/415Project/main/airports.csv"
sc.addFile(url)
sqlContext = SQLContext(sc)

df = sqlContext.read.csv(SparkFiles.get("airports.dat"), header=False, inferSchema= True) 

new_col_names = 'ID', 'Name', 'City', 'Country','IATA', 'ICAO', 'Lat', 'Long', 'Alt', 'Timezone', 'DST', 'Tz database time zone', 'type', 'source'
df = df.toDF(*new_col_names)


#Routes
url2 = "https://raw.githubusercontent.com/AshlynK/415Project/main/routes.csv"
sc.addFile(url2)
sqlContext2 = SQLContext(sc)

df2 = sqlContext2.read.csv(SparkFiles.get("routes.dat"), header=False, inferSchema= True) 

new_col_names2 = 'Airline', 'Airline ID', 'Source Airport', 'Source Airport ID', 'Dest Airport', 'Dest Airport ID', 'Codeshare', 'Stops', 'equipment'
df2 = df2.toDF(*new_col_names2)


#Airlines
url3 = "https://raw.githubusercontent.com/AshlynK/415Project/main/airlines.csv"
sc.addFile(url3)
sqlContext3 = SQLContext(sc)

df3 = sqlContext3.read.csv(SparkFiles.get("airlines.dat"), header=False, inferSchema= True) 

new_col_names3 = "Airline ID", "Airline Name", "Alias", "IATA", "ICAO", "Callsign", "Country", "Active (Y/N)"
df3 = df3.toDF(*new_col_names3)


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
        
        

def find_popular_airport():
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
        
        
        
def airports_by_country():
    area = input("Please enter a country: ")
    
    country_of_area = df.filter(col("Country") == area)
    data_output = list(country_of_area.select('Name').toPandas()['Name'])
    data_output1 = list(country_of_area.select('City').toPandas()['City'])
    
    for i, j in zip(data_output, data_output1):
        print(i, ':', j)
     
    
def country_rankings():
    ranking = input("How many countries would you like to see ranked: ")
    counts = df.groupBy('Country').count()
    counts2 = counts.sort(col('count').desc())
    counts2.show(int(ranking))
    
    
def airport_info():
    city= input("Enter City to output airport(s) info: ")
    
    airports = df.filter(col("City") == city)
    codes = list(airports.select('IATA').toPandas()['IATA'])
    names = list(airports.select('Name').toPandas()['Name'])
    country = list(airports.select('Country').toPandas()['Country'])
    timezone = list(airports.select('Timezone').toPandas()['Timezone'])
    lat = list(airports.select('Lat').toPandas()['Lat'])
    long = list(airports.select('Long').toPandas()['Long'])
    
    print("Airports in", city,":\n")
    for i, j, k, l, m, n in zip(names, country, codes, timezone, lat, long):
        print("Aiport Name: ", i)
        print("Country: ", j)
        print("Code: ", k)
        print("Latitude:", m, "Longitude:", n, "\n")        
     
    
#Main menu function

#if statement to control main menu based on our sample projects, so far this only contains sections 
#from airline/airport facts from option variable above. 
#option 4 shows airport facts
def main_menu1():
    option = input("Welcome to the Airline Search Engine! Plese Select an Option From Below: \n 1) Airline/Airport Information \n 2) Plan a Trip \n 3) FInd an Airport \n 4) Exit\n")
    if option == '1':
        option1 = input("Please Select From Option Below \n 1) View Airports in Your Country \n 2) Countries with Highest Number of Airports \n 3) Cities with Most Airline Traffic \n")
        if option1 == '1':
            airports_by_country()
            go_back()
        if option1 == '2':
            country_rankings()
            go_back()
        if option1 == '3':
            find_popular_airport()
            go_back()
        
        
    if option == '2':
        find_flight()
        go_back()
        
    if option == '3':
        airport_info()
        go_back()

# Option == 4 means exit the program 
    if option == '4':
        print("Goodbye")
        
        
def run_distributed():
    main_menu1()
    
    
run_sequential()
