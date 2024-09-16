
-- Drop the 'people' table if it exists
DROP TABLE IF EXISTS people;

-- Drop the 'places' table if it exists
DROP TABLE IF EXISTS places;

-- Create a table for places
CREATE TABLE places (
    id SERIAL PRIMARY KEY,
    city VARCHAR(255) NOT NULL,
    county VARCHAR(255),
    country VARCHAR(255) NOT NULL
);

-- Create a table for people
CREATE TABLE people (
    id SERIAL PRIMARY KEY,
    given_name VARCHAR(255) NOT NULL,
    family_name VARCHAR(255) NOT NULL,
    date_of_birth DATE,
    place_of_birth VARCHAR(255) NOT NULL
);

-- Index on place_of_birth for faster queries
CREATE INDEX idx_place_of_birth ON people(place_of_birth);