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
       

##################################################################find_airline_traffic alternate##################################################################
#Accepts a ranking parameter and ranks the most popular airports by name and flights in + flights out


def get_popular_airports():
    
    amount = input("How many airports would you like to see ranked: ")
    dict1 = routes_df["Dest Airport"].value_counts()
    dest = dict1.to_dict()
    dict2 = routes_df["Source Airport"].value_counts()
    source = dict2.to_dict()
    final_dictionary =  {x: dest.get(x, 0) + source.get(x, 0) 
                for x in set(dest).union(source)} 
    final_dictionary = sorted(final_dictionary.items(), key=lambda x: x[1], reverse=True)
    print("Top", amount, "airport(s) ranked by flights offered in and out \n")
    traffic = final_dictionary[0:int(amount)]
    new_list = list()
    for element in traffic:
        for index, row in airport_df.iterrows():
            if element[0] == row[3]:
                new_list.append(row[0])
            
    for item, element in zip(new_list, traffic):
        print(item, ":", element[1])

# Function that asks for an airport and gives out info about that airport
def find_an_airport():
    airport = input("Please type in an airport name: ")
    airport = airport.lower() + " airport"
    for index, row in airport_df.iterrows():
        if row["Name"].lower() == airport:
            print(row["Name"],"\nLocation:",row["City"],",", row["Country"],"\nIATA:",row["IATA"],"\nLatitude:",row["Lat"],"\nLongitude:",row["Long"],"\nTimezone:",row["Timezone"])
            
            
#Function that finds flights based on departure and arrival city inputs by user. Outputs Departure airport, to arrival airport, on airlines, with X stops.             
def find_flights():
    #Accepting Input
    depart = input("Please enter a departure city: ")
    arrive = input("Please enter an arriving city: ")
    print("Here are a list of flights from", depart, "to", arrive)
    #defining empty lists to store values for later
    depart_codes = list()
    arrive_codes = list()
    source = list()
    dest = list()
    airline = list()
    stops = list()
    #FInding the arrival and destination airports
    for index, row in airport_df.iterrows():
        if depart == row["City"]:
            depart_codes.append(row["IATA"])
        if arrive == row["City"]:
            arrive_codes.append(row["IATA"])
    for element, item in zip(depart_codes, arrive_codes):
        for index, row in routes_df.iterrows():
            if element == row["Source Airport"] and item == row["Dest Airport"]:
                source.append(row["Source Airport"])
                dest.append(row["Dest Airport"])
                airline.append(row["Airline"])
                stops.append(row["Stops"])
    #finding airline
    names = list()
    for element in airline:
        for index, row in airline_df.iterrows():
            if element == row["IATA"]:
                names.append(row["Airline Name"])
    dest1 = list()
    source1 = list()
    #updating source and destination with actual airport names 
    for element1, element2 in zip(source, dest):
        for index, row in airport_df.iterrows():
            if element1 == row["IATA"]:
                source1.append(row["Name"])
            if element2 == row["IATA"]:
                dest1.append(row["Name"])
    for item1, item2, item3, item4, in zip(source1, dest1, names, stops):
        print("Flight from", item1, "to", item2, "on", item3, "with", item4, "stops")



#if statement to control main menu based on our sample projects, so far this only contains sections 
#from airline/airport facts from option variable above. The only thing needed from section airline/airport facts 
#is city with most airline traffic (option1 == 3)
#option 4 shows airport facts
if option == '1':
    option1 = input("Please Select From Option Below \n 1) View Airports in Your Country/City \n 2) Countries with Highest Number of Airports \n 3) Cities with Most Airline Traffic \n 4) Airport Facts\n")
    if option1 == '1':
        find_airports_by_country()
    if option1 == '2':
        ranking = input("How many cities would you like to see ranked? ")
        frequency = airport_df["Country"].value_counts()
        print(frequency[0:int(ranking)])
    if option1 == '3':
        get_popular_airports()
    if option1 == '4':
        find_an_airport()
        
# Still needing an option == 2, which would be the trip recommendations section, coming soon. 
if option == '2':
    find_flights()

# Option == 3 means exit the program 
if option == '3':
    print("Goodbye")
