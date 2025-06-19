import xml.etree.ElementTree as ET
import json
import os
import re


def extract_cognos_report_info(xml_data):
    """
    Parses Cognos report XML data and extracts metadata about its structure.

    Args:
        xml_data (str): The XML content as a string.

    Returns:
        dict: A dictionary containing the extracted report metadata.
              Returns None if the data cannot be parsed.
    """
    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        print(f"Error parsing XML data: {e}")
        return None

    # Cognos reports have a default namespace. We need to use it to find elements.
    ns = {'d': 'http://developer.cognos.com/schemas/report/16.2/'}

    report_info = {}

    # 1. Extract Report Name
    report_name_element = root.find('d:reportName', ns)
    report_info['report_name'] = report_name_element.text if report_name_element is not None else 'Unknown'

    # 2. Extract Pages and Visuals
    report_info['pages'] = []
    pages = root.findall('.//d:page', ns)
    for page in pages:
        page_info = {
            "page_name": page.get('name'),
            "visuals": []
        }

        # Find crosstabs on the page (can be extended for other visual types like 'list', 'chart')
        visuals = page.findall('.//d:crosstab', ns)
        for visual in visuals:
            query_ref = visual.get('refQuery')
            visual_info = {
                "visual_name": visual.get('name'),
                "visual_type": "crosstab",
                "query_ref": query_ref,
                "rows": [],
                "columns": [],
                "filters": []
            }

            row_items_with_seq = [
                {'seq': i, 'name': item.get('refDataItem')} 
                for i, item in enumerate(visual.findall('.//d:crosstabRows//d:crosstabNodeMember', ns))
            ]
            col_items_with_seq = [
                {'seq': i, 'name': item.get('refDataItem')} 
                for i, item in enumerate(visual.findall('.//d:crosstabColumns//d:crosstabNodeMember', ns))
            ]
            # Remove duplicates while preserving order
            # row_item_names = list(dict.fromkeys(row_item_names_raw))
            # col_item_names = list(dict.fromkeys(col_item_names_raw))


            # Find the associated query to extract expressions and filters
            query = root.find(f".//d:query[@name='{query_ref}']", ns)
            if query is not None:
                # Create a map of dataItem name to its details (expression and type)
                data_item_map = {}
                data_items = query.findall('.//d:selection/d:dataItem', ns)
                for item in data_items:
                    name = item.get('name')
                    expression_element = item.find('d:expression', ns)
                    if name and expression_element is not None and expression_element.text:
                        
                        # Determine the type and aggregation based on the 'aggregate' attribute
                        aggregate_type = item.get('aggregate')
                        column_type = 'dimension'
                        aggregation = None
                        if aggregate_type and aggregate_type != 'none':
                            column_type = 'measure'
                            aggregation = aggregate_type

                        data_item_map[name] = {
                            "expression": expression_element.text.strip(),
                            "type": column_type,
                            "aggregation": aggregation
                        }

                # Populate temporary lists first, which might contain duplicates
                temp_rows = []
                for item_data in row_items_with_seq:
                    name = item_data['name']
                    item_details = data_item_map.get(name, {})
                    row_info = {
                        "seq": item_data['seq'],
                        "name": name,
                        "expression": item_details.get("expression"),
                        "type": item_details.get("type")
                    }
                    if item_details.get('type') == 'measure':
                        row_info['aggregation'] = item_details.get('aggregation')
                    temp_rows.append(row_info)

                temp_cols = []
                for item_data in col_items_with_seq:
                    name = item_data['name']
                    item_details = data_item_map.get(name, {})
                    col_info = {
                        "seq": item_data['seq'],
                        "name": name,
                        "expression": item_details.get("expression"),
                        "type": item_details.get("type")
                    }
                    if item_details.get('type') == 'measure':
                        col_info['aggregation'] = item_details.get('aggregation')
                    temp_cols.append(col_info)

                # --- NEW: Manually filter duplicates based on (name, expression) to preserve order ---
                seen_rows = set()
                unique_rows = []
                for row in temp_rows:
                    # Use a tuple of (name, expression) as the unique key
                    unique_key = (row.get('name'), row.get('expression'))
                    if unique_key not in seen_rows:
                        seen_rows.add(unique_key)
                        unique_rows.append(row)
                visual_info['rows'] = unique_rows

                seen_cols = set()
                unique_cols = []
                for col in temp_cols:
                    # Use a tuple of (name, expression) as the unique key
                    unique_key = (col.get('name'), col.get('expression'))
                    if unique_key not in seen_cols:
                        seen_cols.add(unique_key)
                        unique_cols.append(col)
                visual_info['columns'] = unique_cols


                # Extract filters
                visual_info['filters'] = []
                filter_elements = query.findall('.//d:detailFilter/d:filterExpression', ns)
                for f_element in filter_elements:
                    if f_element.text:
                        full_expression = f_element.text.strip()
                        
                        # Regex to find a pattern like [Namespace].[Subject].[Item]
                        # at the beginning of the filter string.
                        match = re.match(r"(\s*\[.*?\](?:\.\[.*?\])*)", full_expression)
                        column_involved = match.group(1).strip() if match else None

                        filter_info = {
                            "expression": full_expression,
                            "column": column_involved
                        }
                        visual_info['filters'].append(filter_info)

            page_info['visuals'].append(visual_info)
        report_info['pages'].append(page_info)

    return report_info

if __name__ == "__main__":
    # Use the path to your report.xml file
    report_xml_path = r'../../data/report.xml'
    
    xml_content = None
    if not os.path.exists(report_xml_path):
        print(f"Error: File not found at {report_xml_path}")
    else:
        try:
            with open(report_xml_path, 'r', encoding='utf-8') as f:
                xml_content = f.read()
        except Exception as e:
            print(f"Error reading file: {e}")

    if xml_content:
        extracted_data = extract_cognos_report_info(xml_content)
        if extracted_data:
            # Pretty-print the extracted data as a JSON object
            print(json.dumps(extracted_data, indent=2))