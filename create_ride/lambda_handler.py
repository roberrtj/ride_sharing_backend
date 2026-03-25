import json
import requests
import mysql.connector
from token_extractor import get_event_body


global connector
global cursor

# Function to initialize SQL database connection
def initialise_sql_database_connection():
    global connector
    global cursor

    credentials = {}
    with open("database_credentials.json", 'r') as file:
        credentials = json.load(file)

    DB_HOST = credentials['endpoint']
    DB_USER = credentials['admin_name']  
    DB_PASSWORD = credentials['password']  
    DB_NAME = credentials['db_name']  

    try:
        connector = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = connector.cursor()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

# Function to retrieve data from the test_connection table
def test_connection_to_database():  

    try:
        query = "SELECT * FROM test_connection"  
        cursor.execute(query) 
        result = cursor.fetchall()
        
        return {
            'statusCode': 200,
            'body': json.dumps({'data from sql db': result})
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'message': 'error connecting to the sql db; the backends fault',
            'body': json.dumps({'error': str(e)})
        }



def create_ride(data):
    
    ride_admin_user_id = data["ride_admin_user_id"]
    ride_admin_comments = data["ride_admin_comments"]
    current_location = data["current_location"]
    destination = data["destination"]
    capacity = data["capacity"]
    occupancy = data["occupancy"]
    radius_of_acceptance = data["radius_of_acceptance"]
    fuel_type = data["fuel_type"]
    vehicle_type = data["vehicle_type"]

    arrival_time = data['arrival_time']
    CO2_savings = data['CO2_savings']
    start_location = data['start_location']
    departure_time = data['departure_time']
    departure_location_name = data['departure_location_name']
    arrival_location_name = data['arrival_location_name']

    print("json dict is laoding fine")
    
    try:
        
        # SQL Query to Insert a New Ride (Using parameterized queries for security)
        
        #check if user is already providing a ride, and if so, terminate the old one

        cursor.execute("SELECT id FROM Rides WHERE (ride_admin_user_id = %s) AND (finished=FALSE)", (ride_admin_user_id,))
        result = cursor.fetchall()
        
        #now, you may think that it is inefficient to individually change one ride at a time, but if this is done correctly, there should be only one ride that is changed in any one query
        previous_rides = []

        if (result):
            for row in result:
                ride_id = row[0]
                previous_rides.append(ride_id)
                command = f"""
                UPDATE Rides
                    SET finished = TRUE
                    WHERE id = '{ride_id}';
                """
                cursor.execute(command)

        sql_command = """
            INSERT INTO Rides (
                ride_admin_user_id, ride_admin_comments, 
                destination_longitude, destination_latitude, 
                current_location_longitude, current_location_latitude, 
                capacity, occupancy, 
                radius_of_acceptance, fuel_type, vehicle_type, finished, arrival_time, CO2_savings,
                start_location_longitude, start_location_latitude,
                departure_time, departure_location_name,
                arrival_location_name
            ) 
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE,
                %s, %s, %s, %s, %s, %s, %s
            );
        """
        values = (
            ride_admin_user_id, 
            ride_admin_comments, 
            destination['longitude'], destination['latitude'], 
            current_location['longitude'], current_location['latitude'], 
            capacity, occupancy, 
            radius_of_acceptance, fuel_type, vehicle_type,
            arrival_time, CO2_savings,
            start_location['longitude'], start_location['latitude'],
            departure_time,
            departure_location_name, arrival_location_name

        )


        # Execute the query with parameters
        cursor.execute(sql_command, values)
        connector.commit()  # Save changes
        new_ride_id = cursor.lastrowid  # Get the new ride's ID

        rides_deleted = ""
        if 0 < len(previous_rides):
            rides_deleted = ", after terminating rides "
            for old_ride_id in previous_rides:
                rides_deleted += str(old_ride_id) +", "

        return { 
            "new_ride_id": new_ride_id,
            "message": f"Ride created successfully with ID: {new_ride_id}" + rides_deleted
        }
    except Exception as e:
        return {"message": f"An error occurred: {str(e)}", "attempted connection": test_connection_to_database()}




def lambda_handler(event, context):
    
    initialise_sql_database_connection()

    #test_connection_to_database()
    #return { 'message': 'david temp testing message to figure out the user_id issue', 'stage_in_code': 'first line in lambda function'}

     #   URL:"/create_ride/"

    """
    Lambda function to create a ride.

    Parameters (Expected in event['body']):
    --------------------------------------
    - ride_admin_user_id (int): ID of the person managing the ride.
    - ride_admin_comments (str): Notes from the ride provider.
    - start_location (dict[float]): Current ride location (longitude, latitude).
    - current_location (dict[float]): Current ride location (longitude, latitude).
    - destination (dict[float]): Ride destination (longitude, latitude).
    - capacity (int): Maximum number of passengers allowed.
    - occupancy (int): Current number of passengers.
    - radius_of_acceptance (int): Radius in meters for accepting passengers.
    - fuel_type (str): Type of fuel used (e.g., Petrol, Diesel, Electric).
    - vehicle_type (str): Type of vehicle (e.g., Sedan, SUV, Bus).

    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
        dict: Response with status and message.

    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    
    if "body" not in event or event["body"] is None:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing request body"})
        }
    
    print("body of event: ")
    print(event["body"])
    data = {}

    try:
        body_as_str = get_event_body(event)
        data = json.loads(body_as_str)

        if ('user_id' in data):
            data['ride_admin_user_id'] = data['user_id']

    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON format"})
        }

    # Define expected data types for validation
    required_fields = {
        "ride_admin_user_id": str,
        "ride_admin_comments": str,
        "start_location": dict,  # Should contain two float values
        "current_location": dict,  # Should contain two float values
        "destination": dict,  # Should contain two float values
        "capacity": int,
        "occupancy": int,
        "radius_of_acceptance": int,
        "fuel_type": str,
        "vehicle_type": str,
        "arrival_time": str,
        "CO2_savings" : float,
        "departure_time": str,
        "departure_location_name": str,
        "arrival_location_name": str

    }
    """required_fields = {
        
    }"""
    
    # Validate required fields exist
    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"Missing fields: {', '.join(missing_fields)}"})
        }
        
    # Validate fields aren't empty
    empty_fields = [field for field in required_fields if data.get(field) is None or (isinstance(data.get(field), str) and data.get(field).strip() == "")]
    if empty_fields:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"Empty fields: {', '.join(empty_fields)}"})
        }
        
    # Validate data types
    invalid_types = []
    for field, expected_type in required_fields.items():
        value = data[field]

        # Special case: Check if numbers are passed as strings
        if expected_type == int and isinstance(value, str) and value.isdigit():
            data[field] = int(value)  # Convert to integer
        
        '''
        # Adds any fields that are not of the expected type to invalid_types dict
        elif expected_type == dict and not isinstance(value, dict):
            invalid_types.append(field)
        elif not isinstance(value, expected_type):
            invalid_types.append(field)
        '''
    
    # displays any invalid fields if present
    if invalid_types:
        return {"statusCode": 400, "body": json.dumps({"error": f"Invalid data types for fields: {', '.join(invalid_types)}"})}
  
    # variables to add to database
    attempt = create_ride(data)
    
    return {
        "statusCode": 200,
        "body": json.dumps(attempt)
    }

initialise_sql_database_connection()

