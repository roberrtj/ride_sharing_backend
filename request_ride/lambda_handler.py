import mysql.connector
import json
from datetime import timedelta, datetime

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
            'body': json.dumps({'error': str(e)})
        }



def request_ride(data):
    # Calculate timeout expiration time
    current_time = datetime.now()
    request_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    timeout_time = current_time + timedelta(minutes=data['timeout'])
    timeout_string = timeout_time.strftime('%Y-%m-%d %H:%M:%S')

    try:
        # Insert request into the database

        #firstly, check if user has already requested ride and if they have been answered
        checking_query = "SELECT EXISTS(SELECT 1 FROM Requests WHERE user_id = %s AND ride_id = %s)"
        values = (data['requester_id'], data['ride_id'])
        cursor.execute(checking_query, values)

        # Fetch the result (1 if exists, 0 if not)
        result = cursor.fetchone()

        # If result[0] is 1, the row exists, otherwise it doesn't
        print("result[0]: ", result[0])
        
        if (result[0] == 1):
            return {
            "statusCode": 409,
            "body": json.dumps({"message": "Request already sent by this user for this ride"})
        }
        else:
            print("This user has not requested this ride yet, send the request")

        insert_query = """
        INSERT INTO Requests (user_id, ride_id, request_made_timestamp, timeout_timestamp, number_of_people)
        VALUES (%s, %s, %s, %s, %s)
        """
        insert_data = (
            data['requester_id'], data['ride_id'], request_time, timeout_string,
            data['number_of_people']
        )


        cursor.execute(insert_query, insert_data)
        connector.commit()

        new_request_id = cursor.lastrowid

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Request Sent", "request_id": new_request_id})
        }

    except mysql.connector.Error as err:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Database Error: {err}"})
        }



# Lambda handler function
def lambda_handler(event, context):
     #   URL:"/request_ride/"

    """
    Lambda function to request a ride.

    Parameters (Expected in event['body']):
    --------------------------------------
    -   requester_id: (int) the person looking for a ride
    -   ride_id: (int) the ride that the searcher wants to carpool in
    -   timeout: (int) how many minutes will you wait, with no reply, until you discard the request
    -   number_of_people: (int)
    -   requester_comments: (string) if the requester has anything they would like to disclose
    //the location and destinations of the ride, and requesters, are already stored in the "Ride" and "User" tables, 
    so there is no need to store it again here

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
    initialise_sql_database_connection()
    # Check body has been passed into lambda function i.e. non-empty
    if "body" not in event or event["body"] is None:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing request body"})
        }

    
    #ensure body is in a valid JSON format
    data = {}
    body_as_str = "nothing returned from get_event_body(...)"
    try:
        body_as_str = get_event_body(event)
        data = json.loads(body_as_str)

        if ('user_id' in data):
            data['requester_id'] = data['user_id']
        
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON format", 
            "data": data, "body_as_str": body_as_str})
        }

    # Define expected fields and data types for validation
    required_fields = {
        "requester_id": str,
        "ride_id": int,
        "timeout": int,  
        "number_of_people": int,  
        "requester_comments": str
    }
    
    # Validate required fields exist
    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"Missing fields: {', '.join(missing_fields)}"})
        }
        
    # Validate fields aren't empty or don't contain only whitespace characters
    empty_fields = [field for field in required_fields if data.get(field) is None or (isinstance(data.get(field), str) and data.get(field).strip() == "")]
    if empty_fields:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"Empty fields: {', '.join(empty_fields)}"})
        }
        
    # Validate data types for each field(parameter)
    invalid_types = []
    for field, expected_type in required_fields.items():
        value = data[field]

        # Special case: Check if numbers are passed as strings
        if expected_type == int and isinstance(value, str) and value.isdigit():
            data[field] = int(value)  # Convert to integer
       
        # Check if str and ints are passed to the right fields
        # adds any invalid field to invalid_types array
        elif not isinstance(value, expected_type):
            invalid_types.append(field)
    if invalid_types:
        return {"statusCode": 400, "body": json.dumps({"error": f"Invalid data types for fields: {', '.join(invalid_types)}"})}
  
    #variables for each parameter
    requester_id = data.get("requester_id")                 # Get requester_id    
    ride_id = data.get("ride_id")                           # Get ride_id
    timeout = data.get("timeout")                           # Get timeout
    number_of_people = data.get("number_of_people")         # Get number_of_people
    requester_comments = data.get("requester_comments")     # Get requester_comments
    
    #Other checks that need to be done:
    #Doesn't check for extra fields - this may not be necessary to implement
    #Ensure that number_of_people + occupancy.ride_id <= capacity.ride_id
    #!Timeout

    

    try:
        # Fetch ride capacity and current occupancy
        query = """
        SELECT capacity, occupancy FROM Rides WHERE (id = %s)
        """
        cursor.execute(query, (ride_id,))
        ride_data = cursor.fetchone()

        if not ride_data:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "Ride does not exist for ride_id"})
            }

        capacity, occupancy = ride_data

        # Check if the request exceeds the ride's capacity
        if number_of_people + occupancy > capacity:
            overflow = ((number_of_people + occupancy) - capacity)
            return {
                "statusCode": 400,
                "body": json.dumps({"error": f"Request exceeds ride capacity by {overflow} people"})
            }

        return request_ride(data)

    except mysql.connector.Error as err:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Database Error: {err}"})
        }

initialise_sql_database_connection()