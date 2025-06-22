import json
import uuid

# --- Constants ---
SEGOE_UI_FONT_FAMILY = "Segoe UI"

def _create_textbox_config(text):
    """Creates the config for a textbox visual."""
    text_runs = [{"value": text, "textStyle": {"fontWeight": "bold", "fontSize": "28pt", "color": "#ffffff",
                                             "fontFamily": SEGOE_UI_FONT_FAMILY}}]
    return {"visualType": "textbox", "drillFilterOtherVisuals": True, "objects": {
        "general": [{"properties": {"paragraphs": [{"textRuns": text_runs, "horizontalTextAlignment": "center"}]}}]},
            "vcObjects": {"background": [{"properties": {"show": {"expr": {"Literal": {"Value": "false"}}}}}]}}

def _create_image_config(image_name):
    """Creates the config for an image visual."""
    return {"visualType": "image", "drillFilterOtherVisuals": True, "objects": {"general": [{"properties": {
        "imageUrl": {"expr": {"ResourcePackageItem": {"PackageName": "RegisteredResources", "PackageType": 1,
                                                    "ItemName": image_name}}}}}, ], "imageScaling": [
        {"properties": {"imageScalingType": {"expr": {"Literal": {"Value": "'Fill'"}}}}}]}}

def _create_shape_config():
    """Creates the config for a shape visual."""
    return {"visualType": "shape", "drillFilterOtherVisuals": True,
            "objects": {"shape": [{"properties": {"tileShape": {"expr": {"Literal": {"Value": "'rectangle'"}}}}}],
                        "fill": [{"properties": {
                            "fillColor": {"solid": {"color": {"expr": {"Literal": {"Value": "'#0066DD'"}}}}}},
                                "selector": {"id": "default"}}],
                        "outline": [{"properties": {"show": {"expr": {"Literal": {"Value": "false"}}}}}]}}

def _create_data_updated_card_config(table_name, column_name):
    """Creates the config for a card visual showing when data was last updated."""
    projections = {"Values": [{"queryRef": f"Min({table_name}.{column_name})"}]}
    prototypeQuery = {"Version": 2, "From": [{"Name": "d", "Entity": table_name, "Type": 0}], "Select": [{
                                                                                                           "Aggregation": {
                                                                                                               "Expression": {
                                                                                                                   "Column": {
                                                                                                                       "Expression": {
                                                                                                                           "SourceRef": {
                                                                                                                               "Source": "d"}},
                                                                                                                       "Property": column_name}},
                                                                                                               "Function": 3},
                                                                                                           "Name": f"Min({table_name}.{column_name})"}]}
    card_objects = {"labels": [{"properties": {
        "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#ffffff'"}}}}},
        "fontSize": {"expr": {"Literal": {"Value": "12D"}}},
        "fontFamily": {"expr": {"Literal": {"Value": f"'{SEGOE_UI_FONT_FAMILY}'"}}}}}],
                  "categoryLabels": [{"properties": {"show": {"expr": {"Literal": {"Value": "false"}}}}}]}
    vc_objects = {"background": [{"properties": {"show": {"expr": {"Literal": {"Value": "false"}}}}}], "title": [{
                                                                                                                   "properties": {
                                                                                                                       "show": {
                                                                                                                           "expr": {
                                                                                                                               "Literal": {
                                                                                                                                   "Value": "true"}}},
                                                                                                                       "text": {
                                                                                                                           "expr": {
                                                                                                                               "Literal": {
                                                                                                                                   "Value": "'Data Updated on'"}}},
                                                                                                                       "alignment": {
                                                                                                                           "expr": {
                                                                                                                               "Literal": {
                                                                                                                                   "Value": "'center'"}}},
                                                                                                                       "fontColor": {
                                                                                                                           "solid": {
                                                                                                                               "color": {
                                                                                                                                   "expr": {
                                                                                                                                       "Literal": {
                                                                                                                                           "Value": "'#FFFFFF'"}}}}},
                                                                                                                       "fontSize": {
                                                                                                                           "expr": {
                                                                                                                               "Literal": {
                                                                                                                                   "Value": "12D"}}},
                                                                                                                       "fontFamily": {
                                                                                                                           "expr": {
                                                                                                                               "Literal": {
                                                                                                                                   "Value": f"'{SEGOE_UI_FONT_FAMILY}'"}}}}}]}
    return {"visualType": "card", "projections": projections, "prototypeQuery": prototypeQuery,
          "drillFilterOtherVisuals": True, "objects": card_objects, "vcObjects": vc_objects}


def _create_matrix_config(visual_def):
    """
    Creates the config string for a matrix visual by exactly replicating the
    dual-alias system required for tables with both columns and measures.
    """
    projections, select_items_temp = {'Rows': [], 'Columns': [], 'Values': []}, []

    # 1. Gather all fields and identify which tables provide columns vs. measures
    tables_with_measures = set()
    tables_with_columns = set()
    all_tables = set()

    for well_name, proj_key in [('rows', 'Rows'), ('columns', 'Columns'), ('values', 'Values')]:
        is_first_in_well = True
        for field in visual_def.get(well_name, []):
            if not isinstance(field, dict) or not field.get('table') or not field.get('name'): continue

            table_name = field['table']
            all_tables.add(table_name)

            proj_item = {"queryRef": f"{table_name}.{field['name']}"}
            if (well_name == 'rows' or well_name == 'columns') and is_first_in_well:
                proj_item["active"] = True
                is_first_in_well = False
            projections[proj_key].append(proj_item)

            select_items_temp.append(field)

            if field.get('type', 'column').lower() == 'measure':
                tables_with_measures.add(table_name)
            else:
                tables_with_columns.add(table_name)

    # 2. Build the FROM clause with the correct aliasing strategy
    table_aliases = {}
    from_clause = []
    alias_counter = 0

    for table in sorted(list(all_tables)):
        has_measures = table in tables_with_measures
        has_columns = table in tables_with_columns

        if has_measures and has_columns:
            # Create two aliases for tables providing both
            measure_alias = f"t{alias_counter}"
            table_aliases[(table, 'measure')] = measure_alias
            from_clause.append({"Name": measure_alias, "Entity": table, "Type": 0, "Schema": "extension"})
            alias_counter += 1

            column_alias = f"t{alias_counter}"
            table_aliases[(table, 'column')] = column_alias
            from_clause.append({"Name": column_alias, "Entity": table, "Type": 0})
            alias_counter += 1
        else:
            # Create one alias for tables providing only one type
            alias = f"t{alias_counter}"
            field_type = 'measure' if has_measures else 'column'
            table_aliases[(table, field_type)] = alias
            from_item = {"Name": alias, "Entity": table, "Type": 0}
            if has_measures: from_item["Schema"] = "extension"
            from_clause.append(from_item)
            alias_counter += 1

    # 3. Build the SELECT clause using the correct alias for each field
    final_select_items = []
    for item in select_items_temp:
        is_measure = (item.get('type', 'column').lower() == 'measure')
        field_type_key = "Measure" if is_measure else "Column"
        table, name = item.get('table'), item.get('name')
        if not table or not name: continue

        alias_key = (table, 'measure') if is_measure else (table, 'column')
        source_alias = table_aliases.get(alias_key)

        if not source_alias:  # Fallback for tables with only one alias
            source_alias = table_aliases.get((table, 'column')) or table_aliases.get((table, 'measure'))

        if not source_alias:
            print(f"FATAL ERROR: Could not find an alias for {table}.{name}. Halting.")
            return {}

        # No name disambiguation ('1') is needed with the dual-alias system.
        select_item = {
            field_type_key: {"Expression": {"SourceRef": {"Source": source_alias}}, "Property": name},
            "Name": f"{table}.{name}",
            "NativeReferenceName": name
        }
        final_select_items.append(select_item)
    # --- Formatting objects remain the same ---
    matrix_objects = {
        "grid": [{
            "properties": {
                "gridHorizontal": {
                    "expr": {
                        "Literal": {
                            "Value": "false"
                        }
                    }
                }
            }
        }],
        "columnHeaders": [{
            "properties": {
                "bold": {
                    "expr": {
                        "Literal": {
                            "Value": "true"
                        }
                    }
                },
                "fontSize": {
                    "expr": {
                        "Literal": {
                            "Value": "12D"
                        }
                    }
                }
            }
        }],
        "rowHeaders": [{
            "properties": {
                "fontSize": {
                    "expr": {
                        "Literal": {
                            "Value": "10D"
                        }
                    }
                }
            }
        }],
        "values": [{
            "properties": {
                "fontSize": {
                    "expr": {
                        "Literal": {
                            "Value": "12D"
                        }
                    }
                }
            }
        }],
        "general": [{
            "properties": {
                "layout": {
                    "expr": {
                        "Literal": {
                            "Value": "'Tabular'"
                        }
                    }
                }
            }
        }]
    }
    vc_objects = {"stylePreset": [{"properties": {"name": {"expr": {"Literal": {"Value": "'None'"}}}}}], "background": [
        {"properties": {"show": {"expr": {"Literal": {"Value": "true"}}},
                        "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#FFFFFF'"}}}}},
                        "transparency": {"expr": {"Literal": {"Value": "0D"}}}}}]}

    return {"visualType": "pivotTable", "projections": projections, "prototypeQuery": {"Version": 2, "From": from_clause, "Select": final_select_items}, "drillFilterOtherVisuals": True, "objects": matrix_objects, "vcObjects": vc_objects}


def _create_table_config(visual_def):
    """
    Creates the config string for a table visual, replicating the dual-alias system.
    """
    projections, select_items_temp = {'Values': []}, []

    # 1. Gather info
    tables_with_measures = set()
    tables_with_columns = set()
    all_tables = set()

    for field in visual_def.get('fields', []):
        if not isinstance(field, dict) or not field.get('table') or not field.get('name'): continue

        table_name = field['table']
        all_tables.add(table_name)

        projections['Values'].append({"queryRef": f"{table_name}.{field['name']}"})
        select_items_temp.append(field)

        if field.get('type', 'column').lower() == 'measure':
            tables_with_measures.add(table_name)
        else:
            tables_with_columns.add(table_name)

    # 2. Build FROM clause
    table_aliases = {}
    from_clause = []
    alias_counter = 0
    for table in sorted(list(all_tables)):
        has_measures = table in tables_with_measures
        has_columns = table in tables_with_columns
        if has_measures and has_columns:
            m_alias, c_alias = f"t{alias_counter}", f"t{alias_counter + 1}"
            table_aliases[(table, 'measure')], table_aliases[(table, 'column')] = m_alias, c_alias
            from_clause.append({"Name": m_alias, "Entity": table, "Type": 0, "Schema": "extension"})
            from_clause.append({"Name": c_alias, "Entity": table, "Type": 0})
            alias_counter += 2
        else:
            alias, field_type = f"t{alias_counter}", 'measure' if has_measures else 'column'
            table_aliases[(table, field_type)] = alias
            from_item = {"Name": alias, "Entity": table, "Type": 0}
            if has_measures: from_item["Schema"] = "extension"
            from_clause.append(from_item)
            alias_counter += 1

    # 3. Build SELECT clause
    final_select_items = []
    for item in select_items_temp:
        is_measure = (item.get('type', 'column').lower() == 'measure')
        field_type_key = "Measure" if is_measure else "Column"
        table, name = item.get('table'), item.get('name')
        if not table or not name: continue

        alias_key = (table, 'measure') if is_measure else (table, 'column')
        source_alias = table_aliases.get(alias_key) or table_aliases.get((table, 'column')) or table_aliases.get(
            (table, 'measure'))

        if not source_alias:
            print(f"FATAL ERROR: Could not find alias for {table}.{name}. Halting.")
            return {}

        select_item = {field_type_key: {"Expression": {"SourceRef": {"Source": source_alias}}, "Property": name},
                       "Name": f"{table}.{name}", "NativeReferenceName": name}
        final_select_items.append(select_item)

    table_objects = {"grid": [{"properties": {"gridHorizontal": {"expr": {"Literal": {"Value": "false"}}}}}],
                     "columnHeaders": [{"properties": {"bold": {"expr": {"Literal": {"Value": "true"}}},
                                                       "fontSize": {"expr": {"Literal": {"Value": "'12D'"}}}}}],
                     "values": [{"properties": {"fontSize": {"expr": {"Literal": {"Value": "'12D'"}}}}}]}
    vc_objects = {"stylePreset": [{"properties": {"name": {"expr": {"Literal": {"Value": "'None'"}}}}}], "background": [
        {"properties": {"show": {"expr": {"Literal": {"Value": "true"}}},
                        "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#FFFFFF'"}}}}},
                        "transparency": {"expr": {"Literal": {"Value": "0D"}}}}}]}

    return {"visualType": "tableEx", "projections": projections,
            "prototypeQuery": {"Version": 2, "From": from_clause, "Select": final_select_items},
            "drillFilterOtherVisuals": True, "objects": table_objects, "vcObjects": vc_objects}


def _generate_filter_json_string(filters_config):
    """
    Builds the stringified JSON for the 'filters' property.
    Now supports 'Categorical' and 'Advanced' (IsTrue) filter types on both columns and measures.
    """
    if not filters_config:
        return "[]"

    filter_objects = []
    for filter_def in filters_config:
        if not isinstance(filter_def, dict): continue

        field_info = filter_def.get('field', {})
        # Capitalize the type to match the expected JSON keys "Column" or "Measure"
        field_type_key = field_info.get('type', 'column').capitalize()

        if not field_info.get('table') or not field_info.get('name'):
            print(f"Warning: Skipping filter with incomplete field info: {field_info}")
            continue

        # Common base structure for any filter
        filter_obj = {
            "name": uuid.uuid4().hex[:20],
            "expression": {
                field_type_key: {
                    "Expression": {"SourceRef": {"Entity": field_info.get('table')}},
                    "Property": field_info.get('name')
                }
            },
            "howCreated": 1,
            "objects": {}
        }

        filter_type = filter_def.get('filterType')

        # --- Logic for Categorical (Basic) Filters ---
        if filter_type == 'Categorical':
            filter_obj["type"] = "Categorical"
            formatted_values = []
            for v in filter_def.get('values', []):
                if isinstance(v, int):
                    formatted_values.append([{"Literal": {"Value": f"{v}L"}}])
                elif isinstance(v, float):
                    formatted_values.append([{"Literal": {"Value": f"{v}D"}}])
                else:
                    formatted_values.append([{"Literal": {"Value": f"'{v}'"}}])

            filter_obj["filter"] = {
                "Version": 2,
                "From": [{"Name": "t", "Entity": field_info.get('table'), "Type": 0}],
                "Where": [{
                    "Condition": {
                        "In": {
                            "Expressions": [{field_type_key: {"Expression": {"SourceRef": {"Source": "t"}},
                                                              "Property": field_info.get('name')}}],
                            "Values": formatted_values
                        }
                    }
                }]
            }

        # --- ** NEW LOGIC for Advanced Filters ** ---
        elif filter_type == 'Advanced':
            filter_obj["type"] = "Advanced"
            condition = filter_def.get('condition')

            if condition == 'IsTrue':
                # This builds the specific JSON structure for a "Measure is True" filter
                filter_obj["filter"] = {
                    "Version": 2,
                    "From": [{"Name": "t", "Entity": field_info.get('table'), "Type": 0}],
                    "Where": [{
                        "Condition": {
                            "Comparison": {
                                "ComparisonKind": 0,  # 0 = Equal
                                "Left": {field_type_key: {"Expression": {"SourceRef": {"Source": "t"}},
                                                          "Property": field_info.get('name')}},
                                "Right": {"Literal": {"Value": "true"}}  # Boolean literal 'true'
                            }
                        }
                    }]
                }
            else:
                print(f"Warning: Skipping advanced filter with unknown condition: '{condition}'")
                continue

        else:
            print(f"Warning: Skipping filter with unknown type: '{filter_type}'")
            continue

        filter_objects.append(filter_obj)

    return json.dumps(filter_objects)

def _create_footer_shape_config():
    """Creates the config dict for a static footer rectangle with a white fill."""
    return {
        "visualType": "shape",
        "drillFilterOtherVisuals": True,
        "objects": {
            "shape": [{
                "properties": {
                    "tileShape": {"expr": {"Literal": {"Value": "'rectangle'"}}}
                }
            }],
            "rotation": [{
                "properties": {
                    "shapeAngle": {"expr": {"Literal": {"Value": "0L"}}}
                }
            }],
            "outline": [{
                "properties": {
                    "show": {"expr": {"Literal": {"Value": "false"}}}
                }
            }],
            "fill": [
                { "properties": { "show": {"expr": {"Literal": {"Value": "true"}}}} },
                {
                    "properties": {
                        "fillColor": {
                            "solid": {
                                "color": { "expr": {"Literal": {"Value": "'#FFFFFF'"}}}
                            }
                        }
                    },
                    "selector": {"id": "default"}
                }
            ]
        }
    }

def _create_footer_textbox_config(model_name):
    """Creates the config string for the footer textbox with two text runs."""
    text_runs = [
        {"value": " Source: ", "textStyle": {"fontWeight": "bold", "fontSize": "12pt", "fontFamily": "Segoe UI"}},
        {"value": model_name, "textStyle": {"fontSize": "12pt", "fontFamily": "Segoe UI"}}
    ]
    return {"visualType": "textbox", "drillFilterOtherVisuals": True,
          "objects": {"general": [{"properties": {"paragraphs": [{"textRuns": text_runs}]}}]},
          "vcObjects": {"background": [{"properties": {"show": {"expr": {"Literal": {"Value": "false"}}}}}]}}