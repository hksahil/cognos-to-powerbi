import streamlit as st
import xml.etree.ElementTree as ET
import pandas as pd
import json
import re

def parse_xml(xml_file):
    namespaces = []
    tree = ET.parse(xml_file)
    root = tree.getroot()

    for namespace in root.findall('.//{http://www.developer.cognos.com/schemas/bmt/60/12}namespace'):
        namespace_info = {}
        try:
            namespace_info['name'] = namespace.find('{http://www.developer.cognos.com/schemas/bmt/60/12}name').text
        except AttributeError:
            namespace_info['name'] = "N/A"
        try:
            namespace_info['lastChanged'] = namespace.find('{http://www.developer.cognos.com/schemas/bmt/60/12}lastChanged').text
        except AttributeError:
            namespace_info['lastChanged'] = "N/A"
        try:
            namespace_info['lastChangedBy'] = namespace.find('{http://www.developer.cognos.com/schemas/bmt/60/12}lastChangedBy').text
        except AttributeError:
            namespace_info['lastChangedBy'] = "N/A"

        is_business_layer = "Business Layer" in namespace_info['name']

        # Fetch folder details
        folder_details = []
        for folder in namespace.findall('.//{http://www.developer.cognos.com/schemas/bmt/60/12}folder'):
            folder_info = {}
            try:
                folder_info['name'] = folder.find('{http://www.developer.cognos.com/schemas/bmt/60/12}name').text
            except AttributeError:
                folder_info['name'] = "N/A"
            try:
                folder_info['description'] = folder.find('{http://www.developer.cognos.com/schemas/bmt/60/12}description').text or "No description available"
            except AttributeError:
                folder_info['description'] = "N/A"
            try:
                folder_info['lastChanged'] = folder.find('{http://www.developer.cognos.com/schemas/bmt/60/12}lastChanged').text
            except AttributeError:
                folder_info['lastChanged'] = "N/A"
            try:
                folder_info['lastChangedBy'] = folder.find('{http://www.developer.cognos.com/schemas/bmt/60/12}lastChangedBy').text
            except AttributeError:
                folder_info['lastChangedBy'] = "N/A"
            folder_details.append(folder_info)
        namespace_info['folders'] = folder_details

        # Fetch query details
        query_details = []
        for query in namespace.findall('.//{http://www.developer.cognos.com/schemas/bmt/60/12}querySubject'):
            query_info = {}
            try:
                query_info['name'] = query.find('{http://www.developer.cognos.com/schemas/bmt/60/12}name').text
            except AttributeError:
                query_info['name'] = "N/A"
            try:
                query_info['description'] = query.find('{http://www.developer.cognos.com/schemas/bmt/60/12}description').text or "No description available"
            except AttributeError:
                query_info['description'] = "N/A"
            # Fetch SQL query
            try:
                query_info['sql'] = query.find('.//{http://www.developer.cognos.com/schemas/bmt/60/12}dbQuery/{http://www.developer.cognos.com/schemas/bmt/60/12}sql').text
            except AttributeError:
                query_info['sql'] = "N/A"

            # Fetch query item details
            query_items = query.findall('.//{http://www.developer.cognos.com/schemas/bmt/60/12}queryItem')
            query_item_info = []
            for query_item in query_items:
                item_info = {}
                try:
                    item_info['name'] = query_item.find('{http://www.developer.cognos.com/schemas/bmt/60/12}name').text
                except AttributeError:
                    item_info['name'] = "N/A"
                try:
                    item_info['description'] = query_item.find('{http://www.developer.cognos.com/schemas/bmt/60/12}description').text or "No description available"
                except AttributeError:
                    item_info['description'] = "N/A"
                try:
                    item_info['externalName'] = query_item.find('{http://www.developer.cognos.com/schemas/bmt/60/12}externalName').text
                except AttributeError:
                    item_info['externalName'] = "N/A"
                try:
                    item_info['dataType'] = query_item.find('{http://www.developer.cognos.com/schemas/bmt/60/12}datatype').text
                except AttributeError:
                    item_info['dataType'] = "N/A"

                # FIXED: Robustly extract <expression> and its <refobj> child text
                if 1==1:
                    expression_element = query_item.find('{http://www.developer.cognos.com/schemas/bmt/60/12}expression')
                    if expression_element is not None:
                        refobjs = [refobj.text for refobj in expression_element.findall('{http://www.developer.cognos.com/schemas/bmt/60/12}refobj')]
                        item_info['expression'] = " | ".join(refobjs) if refobjs else "N/A"
                        item_info['refobjs'] = refobjs if refobjs else ["N/A"]
                    else:
                        item_info['expression'] = "N/A"
                        item_info['refobjs'] = ["N/A"]
                else:
                    item_info['expression'] = "N/A"
                    item_info['refobjs'] = ["N/A"]

                try:
                    item_info['aggregate'] = query_item.find('{http://www.developer.cognos.com/schemas/bmt/60/12}regularAggregate').text
                except AttributeError:
                    item_info['aggregate'] = "N/A"

                query_item_info.append(item_info)
            query_info['queryItems'] = query_item_info

            query_details.append(query_info)
        namespace_info['queries'] = query_details

        # Fetch shortcut details
        shortcut_details = []
        for shortcut in namespace.findall('.//{http://www.developer.cognos.com/schemas/bmt/60/12}shortcut'):
            shortcut_info = {}
            try:
                shortcut_info['name'] = shortcut.find('{http://www.developer.cognos.com/schemas/bmt/60/12}name').text
            except AttributeError:
                shortcut_info['name'] = "N/A"
            try:
                shortcut_info['description'] = shortcut.find('{http://www.developer.cognos.com/schemas/bmt/60/12}description').text or "No description available"
            except AttributeError:
                shortcut_info['description'] = "N/A"
            try:
                shortcut_info['refobj'] = shortcut.find('{http://www.developer.cognos.com/schemas/bmt/60/12}refobj').text
            except AttributeError:
                shortcut_info['refobj'] = "N/A"
            try:
                shortcut_info['targetType'] = shortcut.find('{http://www.developer.cognos.com/schemas/bmt/60/12}targetType').text
            except AttributeError:
                shortcut_info['targetType'] = "N/A"
            shortcut_details.append(shortcut_info)
        namespace_info['shortcuts'] = shortcut_details

        namespaces.append(namespace_info)

    return namespaces

def main():
    st.title("Cognos Backend Accelerator", help="Extract Metadata of Datasources from Framework Manager")
    
    xml_file = st.file_uploader("Upload XML file", type=["xml"])
    if xml_file is not None:
        namespaces = parse_xml(xml_file)

        presentation_layer_data = []
        database_layer_data = []
        db_sql_lookup = {}
        model_name = "Extracted Model"

        for namespace in namespaces:
            # Try to get a more descriptive model name from a relevant namespace
            if "Business Layer" in namespace['name'] or "Presentation Layer" in namespace['name']:
                model_name = namespace['name'].replace(" (Business Layer)", "").replace(" (Presentation Layer)", "").strip()

            # Process query subjects to populate data layers and lookup
            for query in namespace['queries']:
                is_db_layer_query = query['sql'] != "N/A"
                
                if is_db_layer_query:
                    # Database Layer: items from query subjects with direct SQL
                    for item in query['queryItems']:
                        db_item = {
                            'column': item['name'],
                            'table': query['name'],
                            'sql': query['sql']
                        }
                        database_layer_data.append(db_item)
                        # Create a lookup for SQL using (table_alias, column_name)
                        db_sql_lookup[(query['name'], item['name'])] = query['sql']
                else:
                    # Presentation Layer: items from model query subjects
                    for item in query['queryItems']:
                        presentation_layer_data.append({
                            'column': item['name'],
                            'table': query['name'],
                            'expression': item['expression']
                        })

        # Enrich presentation layer data with SQL and database_name
        for p_item in presentation_layer_data:
            p_item['database_sql'] = 'N/A'
            p_item['database_name'] = 'N/A'
            expression = p_item.get('expression', 'N/A')

            if expression != 'N/A':
                expressions = expression.split(' | ')
                for expr in expressions:
                    match = re.match(r'\[Database Layer\]\.\[(.*?)\]\.\[(.*?)\]', expr.strip())
                    if match:
                        db_table_alias = match.group(1)
                        db_column = match.group(2)
                        
                        sql = db_sql_lookup.get((db_table_alias, db_column))
                        if sql:
                            p_item['database_sql'] = sql
                            
                            # Extract view.table from SQL and construct database_name
                            from_match = re.search(r'FROM\s+([^\s]+)', sql, re.IGNORECASE)
                            if from_match:
                                full_db_object = from_match.group(1).replace('[', '').replace(']', '')
                                p_item['database_name'] = f"{full_db_object}.{db_column}"

                            break # Found a match, no need to check other expressions

        db_to_presentation_mapping = {}
        cognos_to_db_mapping = {}
        for p_item in presentation_layer_data:
            db_name = p_item.get('database_name')
            if db_name and db_name != 'N/A':
                # For cognos_to_db mapping (reverse of db_to_presentation)
                presentation_key = f"{model_name}.{p_item['table']}.{p_item['column']}".lower()
                cognos_to_db_mapping[presentation_key] = db_name

                # For db_to_presentation mapping
                presentation_info = {
                    'presentation_column': f"{p_item['table']}.{p_item['column']}",
                    'table': p_item['table'],
                    'column': p_item['column']
                }
                if db_name not in db_to_presentation_mapping:
                    db_to_presentation_mapping[db_name] = []
                if presentation_info not in db_to_presentation_mapping[db_name]:
                    db_to_presentation_mapping[db_name].append(presentation_info)

        # As per your request, load the JSON file first, then update it.
        try:
            # Step 1: Load column_mappings.json
            with open('../../data/column_mappings.json', 'r') as f:
                output_json = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            # If file doesn't exist or is invalid, create a new structure
            output_json = {
                'model_name': 'N/A',
                'generated_at': 'N/A',
                'mappings': {}
            }
        # Step 2: Update the structure with new data and add db_to_cognos
        output_json['generated_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        output_json['mappings']['db_to_cognos'] = db_to_presentation_mapping
        output_json['mappings']['cognos_to_db'] = cognos_to_db_mapping


        # Write the updated JSON back to column_mappings.json
        with open('../../data/column_mappings.json', 'w') as f:
            json.dump(output_json, f, indent=2)

            
        st.subheader("Generated Mappings")
        st.json(output_json)

        # Provide download button
        st.download_button(
            label="Download Mappings as JSON",
            data=json.dumps(output_json, indent=2),
            file_name=f'{model_name.replace(" ", "_")}_mappings.json',
            mime='application/json'
        )


if __name__ == "__main__":
    main()