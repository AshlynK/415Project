import pyspark
from pyspark.sql.session import SparkSession

from pyspark.sql import SQLContext
from pyspark import SparkFiles
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
