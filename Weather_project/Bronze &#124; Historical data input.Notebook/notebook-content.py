# Fabric notebook source

import requests
import json
import base64
from datetime import datetime, timedelta
import os
import time

%run Bronze | Functions

# API CALL : https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={lon}&type=hour&start={start}&end={end}&appid={API key}

API_KEY = '################################'


# cities and corresponding lat / long
cities = {

'london' : {'lat' : '51.5074','lon' : '0.1278'},
'birmingham' : {'lat' : '52.4862','lon' : '1.8904'},
'manchester' : {'lat' : '53.4808','lon' : '2.2426'},
'liverpool' : {'lat' : '53.4084','lon' : '2.9916'},
'leeds' : {'lat' : '53.8008','lon' : '1.5491'},
'sheffield' : {'lat' : '53.3811','lon' : '1.4701'},
'bristol' : {'lat' : '51.4545','lon' : '2.5879'},
'newcastle_upon_tyne' : {'lat' : '54.9783','lon' : '1.6174'},
'nottingham' : {'lat' : '52.9548','lon' : '1.1581'},
'southampton' : {'lat' : '50.9097','lon' : '1.4044'},
'portsmouth' : {'lat' : '50.8198','lon' : '1.0880'},
'leicester' : {'lat' : '52.6369','lon' : '1.1398'},
'coventry' : {'lat' : '52.4068','lon' : '1.5197'},
'reading' : {'lat' : '51.4543','lon' : '0.9781'},
'kingston_upon_hull' : {'lat' : '53.7676','lon' : '0.3274'},
'york' : {'lat' : '53.9590','lon' : '1.0815'},
'oxford' : {'lat' : '51.7520','lon' : '1.2577'},
'cambridge' : {'lat' : '52.2053','lon' : '0.1218'},
'plymouth' : {'lat' : '50.3755','lon' : '4.1427'},
'derby' : {'lat' : '52.9225','lon' : '1.4746'}

}

# Dictionary for start and end dates

# Get today's date
end_date = datetime.now() 

# Set the start date to 1 year ago 
start_date = end_date - timedelta(weeks=52)

start_dict = {}
end_dict = {}

# Calculate the weekly intervals
week = 0
while start_date < end_date:
    # Calculate start and end of the current week
    week_start = start_date
    week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)

    # Add the start and end timestamps to the dictionaries
    start_dict[f'Week {week+1}'] = to_unix_timestamp(week_start)
    end_dict[f'Week {week+1}'] = to_unix_timestamp(week_end)

    # Move to the next week
    start_date = week_end + timedelta(seconds=1)
    week += 1

# Output the dictionaries
print("Start Dictionary:")
print(start_dict)
print("\nEnd Dictionary:")
print(end_dict)

# Main loop to fetch data for each city and each week
for city, coordinates in cities.items():
    lat = coordinates['lat']
    lon = coordinates['lon']
    
    # Loop through each week
    for week, start_time in start_dict.items():
        end_time = end_dict[week]
        
        try:
            # Define date based on start_time for directory naming
            week_date = datetime.fromtimestamp(start_time).strftime("%Y%m%d")
            
            # Define output directory
            output_dir = f"/lakehouse/default/Files/historical_weather_data/{city}/{week_date}_{week}/"
            
            # Create directory if it does not exist
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # Fetch data from API
            response = fetch_weather_data(lat, lon, start_time, end_time)
            
            if response.status_code == 200:
                weather_data = response.json()
                
                # Define the output file path
                output_file = f"{output_dir}/{week_date}_{week}_weather.json"
                
                # Write the JSON data to the output file
                with open(output_file, "w") as f:
                    json.dump(weather_data, f)
                
                # Check the file size
                file_size = os.path.getsize(output_file)
                print(f"Weather data for {city}, {week} has been exported to: {output_file}")
                print(f"File size: {file_size} bytes")
            else:
                print(f"Error: Failed to get weather data for {city} ({week}) (HTTP Status Code: {response.status_code})")
        
        except requests.RequestException as e:
            print(f"Error: Request failed for {city} ({week}) - {e}")
        except Exception as e:
            print(f"An error occurred for {city} ({week}): {e}")


# all cities exported apart from:
# - York
# - Newcastle
# - Sheffield
# - leeds
# - Liverpool
# - Manchester
# 
# (no data available)
