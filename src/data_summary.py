import json
import sqlalchemy

def connect_to_database():
    # Connect to the PostgreSQL database
    engine = sqlalchemy.create_engine("postgresql://codetest:password@database/codetest")
    connection = engine.connect()
    return connection

def fetch_summary_data(connection):
    # SQL query to retrieve the summary
    query = """
    SELECT places.country, COUNT(people.id) AS count
    FROM places
    LEFT JOIN people ON places.city = people.place_of_birth
    GROUP BY places.country
    """
    result = connection.execute(query).fetchall()
    return result

def convert_to_json(data):
    # Convert the data to a JSON structure
    summary_json = [{"country": row[0], "count": row[1]} for row in data]
    return summary_json

def write_json_to_file(data, output_filename):
    # Write the JSON summary to a file
    with open(output_filename, "w") as output_file:
        json.dump(data, output_file)

def generate_summary():
    connection = connect_to_database()
    summary_data = fetch_summary_data(connection)
    connection.close()
    summary_json = convert_to_json(summary_data)
    output_filename = "/data/summary_output.json"
    write_json_to_file(summary_json, output_filename)
    print("Summary data written to summary_output.json")

if __name__ == "__main__":
    # Run the generate summary function when the script is executed directly
    generate_summary()