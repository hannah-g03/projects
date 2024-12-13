# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a40bc079-4900-4630-97c0-902c8c1e2328",
# META       "default_lakehouse_name": "Weather_lakehouse",
# META       "default_lakehouse_workspace_id": "346e1b64-7681-4e36-aaf6-454ec69177f7"
# META     }
# META   }
# META }

# CELL ********************

import requests
import json
import base64
from datetime import datetime
import os
import time


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# find latest folder

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

api_key = '952ee51c678ec68cca8388c6fa23e4d2'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to get weather data for a given city
def get_weather_data(lat, lon):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    response = requests.get(url)
    return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Loop through all cities and fetch weather data
for city, coordinates in cities.items():
    try:
        # Save current date
        current_date = datetime.now().strftime("%Y%m%d")

        # Define output directory
        output_dir = f"/lakehouse/default/Files/weather_data/{city}/{current_date}/"
        
        # Create directory if it does not exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        lat = coordinates['lat']
        lon = coordinates['lon']
        
        # Get the weather data
        response = get_weather_data(lat, lon)
        
        if response.status_code == 200:
            weather_data = response.json()
            
            # Define the output file path
            output_file = f"{output_dir}/{current_date}_weather.json"
            
            # Write the JSON data to the output file
            with open(output_file, "w") as f:
                json.dump(weather_data, f)
            
            # Check the file size
            file_size = os.path.getsize(output_file)
            print(f"Weather data for {city} has been exported to: {output_file}")
            print(f"File size: {file_size} bytes")
        else:
            print(f"Error: Failed to get weather data for {city} (HTTP Status Code: {response.status_code})")
    
    except requests.RequestException as e:
        print(f"Error: Request failed for {city} - {e}")
    except Exception as e:
        print(f"An error occurred for {city}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
