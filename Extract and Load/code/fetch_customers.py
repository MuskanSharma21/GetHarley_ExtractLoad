import requests
import csv
from io import StringIO

def fetch_customers_data(api_url):
    try:
        # Make GET request to the API
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Since the response is CSV, we can use StringIO to parse it
        csv_data = StringIO(response.text)
        reader = csv.DictReader(csv_data)
        
        # Convert to list of dictionaries
        data = list(reader)
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {str(e)}")
        return None

def save_to_csv(data, output_file):
    if not data:
        print("No data to save")
        return False
    
    try:
        # Get the headers from the first item in the data
        headers = data[0].keys()
        
        # Write to CSV file
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
        return True
    except Exception as e:
        print(f"Error saving to CSV: {str(e)}")
        return False

def main():
    api_url = "https://gist.githubusercontent.com/nicholas-child/4810c9f4892dc63241ed1679d91e912e/raw/customers.csv"
    output_file = "customers_data.csv"
    
    # Fetch data from API
    print("Fetching customer data from API...")
    data = fetch_customers_data(api_url)
    
    if data:
        # Save to CSV
        print("Saving data to CSV...")
        if save_to_csv(data, output_file):
            print(f"Successfully saved data to {output_file}")
            print(f"Total customers retrieved: {len(data)}")
        else:
            print("Failed to save data to CSV")
    else:
        print("Failed to fetch data from API")

if __name__ == "__main__":
    main()
