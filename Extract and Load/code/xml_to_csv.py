import xml.etree.ElementTree as ET
import csv

def xml_to_csv(xml_file, csv_file):
    # Parse XML file
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    # Open CSV file for writing
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow(['conversation_id', 'tag'])
        
        # Extract and write data
        for row in root.findall('row'):
            conversation_id = row.find('conversation_id').text
            tag = row.find('tag').text
            # Handle empty tags
            if tag is None:
                tag = ''
            # Strip whitespace from tag
            tag = tag.strip()
            writer.writerow([conversation_id, tag])

if __name__ == '__main__':
    input_xml = 'conversation_tags_(2).xml'
    output_csv = 'conversation_data.csv'
    
    try:
        xml_to_csv(input_xml, output_csv)
        print(f"Successfully converted {input_xml} to {output_csv}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
