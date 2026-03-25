import json
import requests
import mysql.connector
from datetime import datetime

global connector
global cursor


# Initialise database connection
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
            'body': json.dumps({'data from sql db pickups table': result})
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

    
# Function to query database and find ride associated with ride_id
def find_ride(ride_id):
    """Fetch ride details from Database using ride_id."""
    try:
        # mysql query to retrieve information about a ride where id = ride_id
        # need to figure out how to authenticate user that is logged in
        value = (ride_id,)
        sql_command = """
            SELECT *
            FROM Rides
            WHERE id = %s;
        """
        # Execute the query with parameters
        cursor.execute(sql_command, value)
         # Fetch the result
        row = cursor.fetchone()

        # Ride not found
        if not row:
            return None
        
        ride_dict = {
                "id": row[0],  # Primary Key
                "start_time": row[1].strftime("%Y-%m-%d %H:%M:%S") if isinstance(row[1], datetime) else str(row[1]),
                "ride_admin_user_id": row[2],
                "ride_admin_comments": row[3],
                "destination_longitude": float(row[4]) if row[4] is not None else None,
                "destination_latitude": float(row[5]) if row[5] is not None else None,
                "current_location_longitude": float(row[6]) if row[6] is not None else None,
                "current_location_latitude": float(row[7]) if row[7] is not None else None,
                "start_location_longitude": float(row[8]) if row[8] is not None else None,
                "start_location_latitude": float(row[9]) if row[9] is not None else None,
                "CO2_savings": float(row[10]) if row[10] is not None else None,
                "capacity": row[11],
                "occupancy": row[12],
                "last_update_to_location": row[13].strftime("%Y-%m-%d %H:%M:%S") if isinstance(row[13], datetime) else str(row[13]),
                "radius_of_acceptance": row[14],
                "fuel_type": row[15],
                "vehicle_type": row[16],
                "finished": bool(row[17]),  # Convert tinyint(1) to boolean
                "arrival_time": row[18] if row[18] is not None else "not provided",
                "departure_time": row[19] if row[19] is not None else "not provided",
                "departure_location_name": row[20] if row[20] is not None else "not provided",
                "arrival_location_name": row[21] if row[21] is not None else "not provided"

            }
        return ride_dict
        
    except Exception as e:
        print(f"Error fetching ride: {e}")
        return None
   
    
def lambda_handler(event, context):
     #   URL:"/find_ride/"
    """
    Lambda function to find a ride.

    Parameters (Expected in event['body']):
    --------------------------------------
        - ride_id (int) 
    
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    Success: status code = 2xx (e.g. 200)
        arguments = {message: , start_time: , ride_admin_id: , ride_admin_comments: , start_location_longitude: ,
            start_location_latitude: , current_location_longitude: , current_location_latitude: , 
            destination_longitude: , destination_latitude: , capacity: , occupancy: , 
            radius_of_acceptance: , fuel_type: , vehicle_type: 
        }
        dict: Response with status and message.

    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    initialise_sql_database_connection()
    
    # Check body has been passed into lambda function i.e. non-empty
    if "body" not in event or event["body"] is None:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing request body"})
        }
    
    # Parse input
    try:
        data = json.loads(event["body"])  
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON format"})
        }

    ride_id = data.get("ride_id")  # Extract ride_id from input

    # Checks to ensure ride_id parameter is present as a str
    if ride_id is None or (isinstance(ride_id, str) and ride_id.strip() == ""):
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing ride_id"})
        }
    
    ride_id = int(ride_id)
    
    if not isinstance(ride_id, int):
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid ride_id format"})
        }

    if not ride_id:
        return {"statusCode": 404, "body": json.dumps({"error": "Ride not found"})}
    
    # Get ride from table
    ride = find_ride(ride_id)
    #connector.close()

    if ride is None:
        return {"statusCode": 404, "body": json.dumps({"error": "Ride not found"})}

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Ride found",
            **ride  # Returns all ride details with defaults for missing fields
        }),
    }
    