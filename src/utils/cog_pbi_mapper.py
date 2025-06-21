import streamlit as st
import pandas as pd
import json
from collections import defaultdict
import datetime

def process_mappings(data):
    """
    Processes the mapping data to find Cognos to Power BI links and categorize them.
    """
    try:
        model_name = data.get('model_name', 'N/A')
        generated_at = data.get('generated_at', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        mappings = data['mappings']
        cognos_to_db = mappings['cognos_to_db']
        db_to_powerbi = mappings['db_to_powerbi']
        expression_to_powerbi = mappings.get('expression_to_powerbi', {})
    except KeyError as e:
        st.error(f"Error: The JSON file is missing a required key: {e}.")
        return None

    # This will store the final mapping: {cognos_col: [list_of_powerbi_destinations]}
    cognos_to_powerbi_map = defaultdict(list)
    used_powerbi_columns = set()

    # Helper function to create a consistent string representation for a Power BI item
    def stringify_pbi_item(item):
        if isinstance(item, dict):
            table = item.get('table', 'N/A')
            column = item.get('column', 'N/A')
            return f"{table}.{column}"
        return str(item)

    # Step 1: Create the direct Cognos to Power BI mapping, preserving original objects
    for cognos_col, db_col_source in cognos_to_db.items():
        db_cols = db_col_source if isinstance(db_col_source, list) else [db_col_source]
        for db_col in db_cols:
            powerbi_dest = db_to_powerbi.get(db_col)
            if powerbi_dest:
                powerbi_dests = powerbi_dest if isinstance(powerbi_dest, list) else [powerbi_dest]
                cognos_to_powerbi_map[cognos_col].extend(powerbi_dests)
                used_powerbi_columns.update([stringify_pbi_item(p) for p in powerbi_dests])

    # Step 2: Categorize the generated mappings and prepare final JSON output
    one_to_one = []
    one_to_many = []
    cognos_none_mapped = []
    final_json_map = {}

    for cognos_col, powerbi_mappings in cognos_to_powerbi_map.items():
        unique_mapping_strings = sorted(list(set([stringify_pbi_item(m) for m in powerbi_mappings])))

        if len(unique_mapping_strings) == 1:
            # This is a one-to-one mapping
            pbi_mapping_object = powerbi_mappings[0]
            pbi_string = unique_mapping_strings[0]
            
            try:
                pbi_table, pbi_column = pbi_string.split('.', 1)
                one_to_one.append({
                    "Cognos Column": cognos_col,
                    "Power BI Table": pbi_table.strip(),
                    "Power BI Column": pbi_column.strip()
                })
            except ValueError:
                one_to_one.append({
                    "Cognos Column": cognos_col,
                    "Power BI Table": "N/A (Invalid Format)",
                    "Power BI Column": pbi_string
                })
            
            # If the original mapping was a dictionary, add it to our final JSON export
            if isinstance(pbi_mapping_object, dict):
                final_json_map[cognos_col] = pbi_mapping_object

        elif len(unique_mapping_strings) > 1:
            one_to_many.append({
                "Cognos Column": cognos_col,
                "Mapped Power BI Options": ", ".join(unique_mapping_strings)
            })

    # Find Cognos columns that had no final Power BI mapping
    all_cognos_cols = set(cognos_to_db.keys())
    mapped_cognos_cols = set(cognos_to_powerbi_map.keys())
    unmapped_cognos_cols = all_cognos_cols - mapped_cognos_cols
    for col in sorted(list(unmapped_cognos_cols)):
        cognos_none_mapped.append({"Unmapped Cognos Column": col})

    # Step 3: Find Power BI columns that were never mapped from any Cognos column
    all_powerbi_columns = set()
    all_pbi_sources = list(db_to_powerbi.values()) + list(expression_to_powerbi.values())
    for dest in all_pbi_sources:
        dests_list = dest if isinstance(dest, list) else [dest]
        all_powerbi_columns.update([stringify_pbi_item(p) for p in dests_list])

    unmapped_powerbi_cols = all_powerbi_columns - used_powerbi_columns
    powerbi_none_mapped = []
    for col in sorted(list(unmapped_powerbi_cols)):
        try:
            pbi_table, pbi_column = col.split('.', 1)
            powerbi_none_mapped.append({
                "Unmapped Power BI Table": pbi_table.strip(),
                "Unmapped Power BI Column": pbi_column.strip()
            })
        except ValueError:
            powerbi_none_mapped.append({
                "Unmapped Power BI Table": "N/A (Invalid Format)",
                "Unmapped Power BI Column": col
            })

    # Construct the final JSON object for download
    final_json_output = {
        "model_name": model_name,
        "generated_at": generated_at,
        "cognos_to_powerbi": final_json_map
    }

    return {
        "one_to_one": pd.DataFrame(one_to_one),
        "one_to_many": pd.DataFrame(one_to_many),
        "cognos_none_mapped": pd.DataFrame(cognos_none_mapped),
        "powerbi_none_mapped": pd.DataFrame(powerbi_none_mapped),
        "final_json": final_json_output
    }


def main():

    # --- Streamlit App UI ---
    st.set_page_config(layout="wide")
    st.title("Cognos to Power BI Mapping Analyzer")
    st.markdown("""
    This application analyzes the `data/column_mappings.json` file to generate and categorize the complete mapping from Cognos to Power BI.
    """)

    try:
        # Load data from local file
        with open('data/column_mappings.json', 'r', encoding='utf-8') as f:
            data = json.load(f)

        with st.spinner('Processing mappings...'):
            results = process_mappings(data)

        if results:
            st.success("Processing complete! Here are the results:")

            # Display One-to-One Mappings
            st.header(f"✅ One-to-One Mapped ({len(results['one_to_one'])})")
            if not results["one_to_one"].empty:
                st.dataframe(results["one_to_one"], use_container_width=True)
            else:
                st.info("No direct one-to-one mappings were found.")

            # Display One-to-Many Mappings
            st.header(f"⚠️ One-to-Many Mapped (Ambiguous) ({len(results['one_to_many'])})")
            if not results["one_to_many"].empty:
                st.dataframe(results["one_to_many"], use_container_width=True)
            else:
                st.info("No one-to-many mappings were found.")

            # Display Unmapped Cognos Columns
            st.header(f"❌ Unmapped Cognos Columns ({len(results['cognos_none_mapped'])})")
            if not results["cognos_none_mapped"].empty:
                st.dataframe(results["cognos_none_mapped"], use_container_width=True)
            else:
                st.info("All Cognos columns were successfully mapped to a Power BI destination.")

            # Display Unmapped Power BI Columns
            st.header(f"❓ Unmapped Power BI Columns ({len(results['powerbi_none_mapped'])})")
            if not results["powerbi_none_mapped"].empty:
                st.dataframe(results["powerbi_none_mapped"], use_container_width=True)
            else:
                st.info("All available Power BI columns were used in a mapping.")
            
            # Add download button for the final JSON
            if results['final_json']['cognos_to_powerbi']:
                st.header("⬇️ Download Full Mapping")
                final_json_string = json.dumps(results['final_json'], indent=2)
                st.download_button(
                    label="Download one-to-one mapping as JSON",
                    data=final_json_string,
                    file_name="cognos_to_powerbi_mapping.json",
                    mime="application/json"
                )
                st.json(results['final_json'])


    except FileNotFoundError:
        st.error("Error: `data/column_mappings.json` not found. Please make sure the file exists in the `data` directory.")
    except json.JSONDecodeError:
        st.error("Error: The file `data/column_mappings.json` is not a valid JSON file.")
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()