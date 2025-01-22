import requests
import csv
import json

def fetch_clinics_data(api_url):
    try:
        # Make GET request to the API
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
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
    api_url = "https://gh-data-eng-exercise-api-e8769da4c13b.herokuapp.com/api/clinics"
    output_file = "clinics_data.csv"
    
    # Fetch data from API
    print("Fetching data from API...")
    data = fetch_clinics_data(api_url)
    
    if data:
        # Save to CSV
        print("Saving data to CSV...")
        if save_to_csv(data, output_file):
            print(f"Successfully saved data to {output_file}")
        else:
            print("Failed to save data to CSV")
    else:
        print("Failed to fetch data from API")

if __name__ == "__main__":
    main()
