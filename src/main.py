from data_ingestion import ingest_data
from data_summary import generate_summary

def main():
    # Data ingestion logic
    ingest_data()

    # Data summary logic
    generate_summary()

if __name__ == "__main__":
    main()