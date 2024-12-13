# Fabric notebook source

#Flatten array of structs and structs
def flatten(df):
   # compute Complex Fields (Lists and Structs) in Schema   
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
      # if StructType then convert all sub element to columns.
      # i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
    
      # if ArrayType then add the Array Elements as Rows using the explode function
      # i.e. explode Arrays
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))
    
      # recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df

def convert_to_iso(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).isoformat()


# Load and clean JSON data - all data
def load_and_clean_data(data_path):
    all_data = []
    
    # Loop through each city's directory
    for city in os.listdir(data_path):
        city_path = os.path.join(data_path, city)
        
        # Loop through each date within the city directory
        for date_dir in os.listdir(city_path):
            date_path = os.path.join(city_path, date_dir)
            
            # Load JSON file
            for file_name in os.listdir(date_path):
                if file_name.endswith(".json"):
                    file_path = os.path.join(date_path, file_name)
                    
                    with open(file_path, "r") as file:
                        data = json.load(file)
                        data["city"] = city  # Add city information to the dataset
                        
                        # Append to all_data list
                        all_data.append(data)
                        
    return all_data


# Load and clean JSON data - newest folder
def load_and_clean_newest_data(data_path):
    all_data = []
    
    # Loop through each city's directory
    for city in os.listdir(data_path):
        city_path = os.path.join(data_path, city)
        
        # Find the latest date folder within the city directory
        latest_date_folder = find_latest_date_folder(city_path)
        latest_date_path = os.path.join(city_path, latest_date_folder)
            
            # Load JSON file
        for file_name in os.listdir(latest_date_path):
            if file_name.endswith(".json"):
                file_path = os.path.join(latest_date_path, file_name)
                    
                with open(file_path, "r") as file:
                    data = json.load(file)
                    data["city"] = city  
                        
                    # Append to all_data list
                    all_data.append(data)
                        
    return all_data

def find_latest_date_folder(directory_path):
    #List all folders in the directory
    folders = [f for f in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, f))]

    # Folder names in YYYYMMDD format
    sorted_folders = sorted(folders, key=lambda x: int(x))
    
    # Sort for latest date folder
    latest_date_folder = sorted_folders[-1]
    
    return latest_date_folder

# Function to convert to UNIX timestamp
def to_unix_timestamp(dt):
    return int(time.mktime(dt.timetuple()))

# Function to fetch weather data
def fetch_weather_data(lat, lon, start, end):
    url = f"https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={lon}&type=hour&start={start}&end={end}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    return response

# Normalize historical data
def normalize_historical_data(df):
    # Explode to get each weather record as a separate row
    df = df.withColumn("record", explode(col("list")))

    # Extract relevant fields from each weather record
    flattened_df = df.select(
        col("city_id"),
        col("record.dt").alias("dt"),
        col("record.main.temp").alias("main_temp"),
        col("record.main.feels_like").alias("main_feels_like"),
        col("record.main.temp_min").alias("main_temp_min"),
        col("record.main.temp_max").alias("main_temp_max"),
        col("record.main.pressure").alias("main_pressure"),
        col("record.main.humidity").alias("main_humidity"),
        col("record.wind.speed").alias("wind_speed"),
        col("record.wind.deg").alias("wind_deg"),
        col("record.clouds.all").alias("clouds_all"),
        col("record.weather.id").alias("weather_id"),
        col("record.weather.main").alias("weather_main"),
        col("record.weather.description").alias("weather_description"),
        col("record.weather.icon").alias("weather_icon"),
        col("city")
    )

    # Convert Unix timestamp to TimestampType
    flattened_df = flattened_df.withColumn("dt", col("dt").cast(TimestampType()))
    
    return flattened_df

# Convert kelvin to celsius
def kelvin_to_celsius(kelvin):
    
    # subtract 273.15 for values > 100
    df[column_name] = df[column_name].apply(lambda x: x - 273.15 if x > 100 else x)
    
    return df
    
