import csv
import sqlalchemy
from dateutil.parser import parse

def is_date_valid(date):
    try:
        # Try to parse the date using dateutil.parser.parse
        parse(date)
        return True
    except ValueError:
        # If parsing fails, return False
        return False

def is_not_empty(value):
    # Check if the string is not empty after stripping leading and trailing whitespace
    return value.strip() != ""

def is_duplicate_people(connection, given_name, family_name, date_of_birth, place_of_birth):
    # Construct a SQL query to count records with the same attributes
    query = sqlalchemy.text(
        "SELECT COUNT(*) FROM people WHERE given_name = :given_name AND family_name = :family_name "
        "AND date_of_birth = :date_of_birth AND place_of_birth = :place_of_birth"
    )
    # Execute the query and retrieve the result as a scalar value
    result = connection.execute(query, given_name=given_name, family_name=family_name,
                               date_of_birth=date_of_birth, place_of_birth=place_of_birth).scalar()
    # Return True if there are more than 0 records with the same attributes (i.e., it's a duplicate)
    return result > 0

def ingest_data():
    try:
        # Connect to the database
        engine = sqlalchemy.create_engine("postgresql://codetest:password@database/codetest")
        connection = engine.connect()

        metadata = sqlalchemy.MetaData(engine)

        # Make ORM objects to refer to the 'people' and 'places' tables
        People = sqlalchemy.Table('people', metadata, autoload=True, autoload_with=engine)
        Places = sqlalchemy.Table('places', metadata, autoload=True, autoload_with=engine)

        # Read the 'places.csv' data file into the 'places' table
        with open('/data/places.csv') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                # Data type validation
                city = row['city']
                county = row['county']
                country = row['country']

                if not (is_not_empty(city) and is_not_empty(county) and is_not_empty(country)):
                    # Missing value check
                    print("Skipped a record in places.csv due to missing values.")
                    continue

                connection.execute(Places.insert().values(
                    city=city,
                    county=county,
                    country=country
                ))

        # Read the 'people.csv' data file into the 'people' table
        with open('/data/people.csv') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                # Data type validation
                given_name = row['given_name']
                family_name = row['family_name']
                date_of_birth = row['date_of_birth']
                place_of_birth = row['place_of_birth']

                if not (is_not_empty(given_name) and is_not_empty(family_name) and
                        is_date_valid(date_of_birth) and is_not_empty(place_of_birth)):
                    # Missing value check and data type validation for date_of_birth
                    print("Skipped a record in people.csv due to missing values or invalid date.")
                    continue

                if is_duplicate_people(connection, given_name, family_name, date_of_birth, place_of_birth):
                    print("Skipped a record in people.csv due to duplicate entry.")
                    continue

                connection.execute(People.insert().values(
                    given_name=given_name,
                    family_name=family_name,
                    date_of_birth=date_of_birth,
                    place_of_birth=place_of_birth
                ))

        connection.close()
        print("Data ingestion complete.")

    except Exception as e:
        print("An error occurred during data ingestion:", str(e))

if __name__ == "__main__":
    # Run the data ingestion function when the script is executed directly
    ingest_data()
