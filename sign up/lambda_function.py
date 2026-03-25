import json
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
            'body': json.dumps({'error': str(e)})
        }



def create_user(data):
    name = data["name"]
    email = data["email"]
    age = data["age"]
    gender = data.get("gender", None)  # Gender is optional
    phone_number = data.get("phone_number", None)  # Phone number is optional

    average_rating = data.get("average_rating", None)  # Optional field
    CO2_savings = data.get("CO2_savings", 0.00000)  # Default to 0 if not provided
    current_location = data["current_location"]
    destination = data["destination"]

    try:
        
        # SQL Query to Insert a New User (Using parameterized queries for security)
        sql_command = """
            INSERT INTO Users (
                id, name, email, age, gender, phone_number, average_rating, CO2_savings,
                current_location_longitude, current_location_latitude,
                destination_longitude, destination_latitude
            ) 
            VALUES (
                %s, %s, %s, %s, %s, 
                %s, %s, %s, 
                %s, %s, 
                %s, %s
            );
        """

        user_id = email
        if ('user_id' in data):
            user_id = data['user_id']

        values = (
            user_id,
            name, 
            email, 
            age, 
            gender, 
            phone_number, 
             
            average_rating, 
            CO2_savings,
            current_location["longitude"], current_location["latitude"],
            destination["longitude"], destination["latitude"]
        )

        # Execute the query with parameters
        cursor.execute(sql_command, values)
        connector.commit()  # Save changes
        #user_id = cursor.lastrowid  # Get the new user's ID

        return { 
            "user_id": user_id,
            "message": f"User created successfully with ID: {user_id}"
        }
    except Exception as e:
        return {"message": f"An error occurred: {str(e)}", "attempted connection": test_connection_to_database()}


def lambda_handler(event, context):
    """
    Lambda function to create a user.

    Parameters (Expected in event['body']):
    --------------------------------------
    - username (str): The user's username.
    - email (str): The user's email.
    - password (str): The user's password.

    event: dict, required
        API Gateway Lambda Proxy Input Format

    context: object, required
        Lambda Context runtime methods and attributes

    Returns
    ------
        dict: Response with status and message.
    """

    initialise_sql_database_connection()

    if "body" not in event or event["body"] is None:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing request body"})
        }
    
    try:
        body_as_str = get_event_body(event)
        data = json.loads(body_as_str)  # Parse JSON input
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON format", "event": event, "body": body_as_str})
        }

    # get user_id from body
    user_id = data.get("user_id")
    if (user_id is None):
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing user_id"})
        }

    # Query database to check if user already exists
    sql_command = """
        SELECT * FROM Users WHERE id = %s
    """
    cursor.execute(sql_command, (user_id,))
    result = cursor.fetchone()
    
    if (result is not None):
        print("User already exists")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "User already exists", "user_id": user_id})
        }
    else:
        # Call create_user to insert new user
        attempt = create_user(data)
        print("New user created")
    
    return {
        "statusCode": 200,
        "body": json.dumps(attempt)
    }


initialise_sql_database_connection()

