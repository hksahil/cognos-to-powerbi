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
        # Find both crosstabs and lists on the page using an XPath OR operator
         # --- FIX: Use two separate findall calls as ElementTree does not support the '|' operator ---
        crosstabs = page.findall('.//d:crosstab', ns)
        lists = page.findall('.//d:list', ns)
        visuals = crosstabs + lists # Combine the results into a single list

        
        for visual in visuals:
            query_ref = visual.get('refQuery')
            
            # Determine the visual type from the XML tag
            visual_tag = visual.tag.replace(f'{{{ns["d"]}}}', '')
            visual_type = "table" if visual_tag == 'list' else "crosstab"

            visual_info = {
                "visual_name": visual.get('name'),
                "visual_type": visual_type,
                "query_ref": query_ref,
                "rows": [],
                "columns": [],
                "filters": []
            }

            row_items_with_seq = []
            col_items_with_seq = []

            # --- Conditional Parsing Logic ---
            if visual_type == 'crosstab':
                # Find all descendant nodes within the rows section
                all_row_nodes = visual.findall('.//d:crosstabRows//*', ns)
                # Filter for elements that actually define a data item on a row
                row_defining_elements = [
                    node for node in all_row_nodes 
                    if node.tag in (f'{{{ns["d"]}}}crosstabNodeMember', f'{{{ns["d"]}}}crosstabTotal')
                ]
                row_items_with_seq = [
                    {'seq': i, 'name': item.get('refDataItem')} 
                    for i, item in enumerate(row_defining_elements)
                ]

                # Find all descendant nodes within the columns section
                all_col_nodes = visual.findall('.//d:crosstabColumns//*', ns)
                # Filter for elements that actually define a data item on a column
                col_defining_elements = [
                    node for node in all_col_nodes
                    if node.tag in (f'{{{ns["d"]}}}crosstabNodeMember', f'{{{ns["d"]}}}crosstabTotal')
                ]
                col_items_with_seq = [
                    {'seq': i, 'name': item.get('refDataItem')} 
                    for i, item in enumerate(col_defining_elements)
                ]
            
            elif visual_type == 'table':
                # For tables, we only parse columns. The 'rows' list will remain empty.
                list_columns = visual.findall('.//d:listColumns/d:listColumn', ns)
                temp_col_items = []
                for i, col_node in enumerate(list_columns):
                    # Find the dataItemValue which holds the reference to the query item
                    data_item_value = col_node.find('.//d:dataItemValue', ns)
                    if data_item_value is not None:
                        ref_name = data_item_value.get('refDataItem')
                        if ref_name:
                            temp_col_items.append({'seq': i, 'name': ref_name})
                col_items_with_seq = temp_col_items


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
                detail_filters = query.findall('.//d:detailFilter', ns)

                for detail_filter in detail_filters:
                    filter_info = {}
                    
                    # --- NEW: Handle the structured <filterInValues> format ---
                    in_filter = detail_filter.find('.//d:filterInValues', ns)
                    if in_filter is not None:
                        ref_data_item = in_filter.get('refDataItem')
                        if ref_data_item and ref_data_item in data_item_map:
                            column_expression = data_item_map[ref_data_item].get('expression')
                            values = [v.text for v in in_filter.findall('.//d:filterValue', ns) if v.text]
                            
                            if column_expression and values:
                                # Reconstruct the expression string for consistency
                                values_str = "', '".join(values)
                                full_expression = f"{column_expression} in ('{values_str}')"
                                
                                filter_info = {
                                    "expression": full_expression,
                                    "column": column_expression
                                }

                    # --- FALLBACK: Handle the raw <filterExpression> format ---
                    else:
                        f_element = detail_filter.find('.//d:filterExpression', ns)
                        if f_element is not None and f_element.text:
                            full_expression = f_element.text.strip()
                            match = re.match(r"(\s*\[.*?\](?:\.\[.*?\])*)", full_expression)
                            column_involved = match.group(1).strip() if match else None
                            filter_info = {
                                "expression": full_expression,
                                "column": column_involved
                            }

                    if filter_info:
                        visual_info['filters'].append(filter_info)

            page_info['visuals'].append(visual_info)
        report_info['pages'].append(page_info)

    return report_info

if __name__ == "__main__":
    # Use the path to your report.xml file
    report_xml_path = r'../../data/table_rep.xml'
    
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