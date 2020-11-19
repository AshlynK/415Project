# %load sequential.py
# import libaries
import pandas as pd
from collections import Counter

#Most of this is referencing the sample project on blackboard: 
#https://learn.wsu.edu/webapps/blackboard/execute/content/file?cmd=view&content_id=_4209658_1&course_id=_263777_1

#Reading in data frames from "jpatokal" and adding column headings manually 

airport_df = pd.read_csv("https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat",
                        names = ['ID', 'Name', 'City', 'Country','IATA', 'ICAO', 'Lat', 'Long', 'Alt', 
              'Timezone', 'DST', 'Tz database time zone', 'type', 'source'], index_col = 0)

routes_df = pd.read_csv("https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat",
                        names = ['Airline', 'Airline ID', 'Source Airport', 'Source Airport ID',
              'Dest Airport', 'Dest Airport ID', 'Codeshare', 'Stops', 'equipment'])

airline_df = pd.read_csv("https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat",
                         names = ["Airline ID", "Airline Name", "Alias", "IATA", "ICAO", "Callsign", "Country", "Active (Y/N)"])

#Setting up the main menu

option = input("Welcome to the Airline Search Engine! Plese Select an Option From Below: \n 1) Airline/Airport Facts \n 2) Plan a Trip \n 3) Exit\n")


########################################## Airline Statistics Functions #############################################    


## Function that asks for a country and outputs all the airports in the country, with corresponding location city
def find_airports_by_country():
    area = input("Please enter a country: ")
    print("The following Airports are in", area)
    for index, row in airport_df.iterrows():       
        if row["Country"] == area:
            print(row["Name"], "in", row["City"])
       
## Function that finds the city with most airline traffic
def find_airline_traffic():
    new_dict = {}
    for index1, row1 in airport_df.iterrows():
        for index2, row2 in routes_df.iterrows():
            print("Source airport ID:",row2["Source Airport ID"])
            print("airport ID:",row1["ID"])
            print("Dest Airport Id:", row2["Dest Airport ID"])
#            if str(row2["Source Airport ID"]) == str(row1["ID"]) or str(row2["Dest Airport ID"]) == str(row1["ID"]):
#                if row1["City"] in new_dict:
#                    new_dict[row1["City"]] += 1
#                else:
#                    new_dict[row1["City"]] = 1 
#    keymax = max(new_dict, key=new_dict.get)
#    print("City with most traffic:", keymax)
    
    
#if statement to control main menu based on our sample projects, so far this only contains sections 
#from airline/airport facts from option variable above. The only thing needed from section airline/airport facts 
#is city with most airline traffic (option1 == 3)

if option == '1':
    option1 = input("Please Select From Option Below \n 1) View Airports in Your Country/City \n 2) Countries with Highest Number of Airports \n 3) Cities with Most Airline Traffic \n")
    if option1 == '1':
        find_airports_by_country()
    if option1 == '2':
        ranking = input("How many cities would you like to see ranked? ")
        frequency = airport_df["Country"].value_counts()
        print(frequency[0:int(ranking)])
    if option1 == '3':
        find_airline_traffic()
# Still needing an option == 2, which would be the trip recommendations section, coming soon. 

# Option == 3 means exit the program 
if option == '3':
    print("Goodbye")
