import streamlit as st
import pandas as pd
import json
from collections import defaultdict
import datetime
import re


def normalize_name(name):
    """Converts a name to a simplified, comparable format (lowercase, alphanumeric)."""
    if not isinstance(name, str):
        return ""
    return re.sub(r'[^a-z0-9]', '', name.lower())

def stringify_pbi_item(item):
    """Creates a consistent 'table.column' string representation for a Power BI item."""
    if isinstance(item, dict):
        table = item.get('table', 'N/A')
        column = item.get('column', 'N/A')
        return f"{table}.{column}"
    return str(item)

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
    one_to_many_for_ui = []
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
            
            if isinstance(pbi_mapping_object, dict):
                final_json_map[cognos_col] = pbi_mapping_object

        elif len(unique_mapping_strings) > 1:
            # Add to the summary table data
            one_to_many.append({
                "Cognos Column": cognos_col,
                "Mapped Power BI Options": ", ".join(unique_mapping_strings)
            })

            # Prepare data for the interactive UI
            unique_objects = []
            seen_strings = set()
            for pbi_obj in powerbi_mappings:
                pbi_str = stringify_pbi_item(pbi_obj)
                if pbi_str not in seen_strings:
                    if isinstance(pbi_obj, dict):
                        unique_objects.append(pbi_obj)
                        seen_strings.add(pbi_str)
            
            if unique_objects:
                one_to_many_for_ui.append({
                    "cognos_col": cognos_col,
                    "options": unique_objects
                })

    # Find Cognos columns that had no final Power BI mapping
    all_cognos_cols = set(cognos_to_db.keys())
    mapped_cognos_cols = set(cognos_to_powerbi_map.keys())
    unmapped_cognos_cols = all_cognos_cols - mapped_cognos_cols
    for col in sorted(list(unmapped_cognos_cols)):
        cognos_none_mapped.append({"Unmapped Cognos Column": col})

    # Step 3: Find Power BI columns that were never mapped from any Cognos column
    all_powerbi_columns = set()
    pbi_string_to_object_map = {}
    all_pbi_sources = list(db_to_powerbi.values()) + list(expression_to_powerbi.values())
    for dest in all_pbi_sources:
        dests_list = dest if isinstance(dest, list) else [dest]
        for p in dests_list:
            p_str = stringify_pbi_item(p)
            all_powerbi_columns.add(p_str)
            if isinstance(p, dict):
                pbi_string_to_object_map[p_str] = p

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
        "mappings": {
            "cognos_to_powerbi": final_json_map
        }
    }

    return {
        "one_to_one": pd.DataFrame(one_to_one),
        "one_to_many": pd.DataFrame(one_to_many),
        "one_to_many_for_ui": one_to_many_for_ui,
        "cognos_none_mapped": pd.DataFrame(cognos_none_mapped),
        "powerbi_none_mapped": pd.DataFrame(powerbi_none_mapped),
        "final_json": final_json_output,
        "pbi_string_to_object_map": pbi_string_to_object_map
    }


def main():

    # --- Streamlit App UI ---
    st.set_page_config(layout="wide")
    st.title("Cognos to Power BI Mapping Analyzer")
    st.markdown("""
    This application analyzes the `data/column_mappings.json` file to generate and categorize the complete mapping from Cognos to Power BI.
    """)

    # Display and clear any messages from the previous run
    if 'message' in st.session_state:
        st.success(st.session_state.message)
        del st.session_state.message

    try:
        # Load data from local file
        with open('data/column_mappings.json', 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Initialize state on first run or if data changes.
        if 'results' not in st.session_state:
            with st.spinner('Processing mappings...'):
                st.session_state.results = process_mappings(data)
        
        # Initialize UI state for manual resolver
        if 'show_manual_resolver' not in st.session_state:
            st.session_state.show_manual_resolver = False

        results = st.session_state.results

        if results:
            # Display One-to-One Mappings
            st.header(f"✅ One-to-One Mapped ({len(results['one_to_one'])})")
            if not results["one_to_one"].empty:
                st.dataframe(results["one_to_one"], use_container_width=True)
            else:
                st.info("No direct one-to-one mappings were found.")

            # Display One-to-Many Mappings Table
            st.header(f"⚠️ One-to-Many Mapped (Ambiguous) ({len(results['one_to_many'])})")
            if not results["one_to_many"].empty:
                st.dataframe(results["one_to_many"], use_container_width=True)
            else:
                st.info("No one-to-many mappings were found.")

            # Collapsible Mapping Resolution section
            with st.expander(f"⚙️ Resolve Ambiguous Mappings ({len(results['one_to_many_for_ui'])})", expanded=True):
                if results['one_to_many_for_ui']:
                    col1, col2 = st.columns(2)
                    
                    # --- AUTO-RESOLVE BUTTON ---
                    if col1.button("Attempt Auto-Resolution", use_container_width=True):
                        resolved_mappings = {}
                        still_ambiguous_ui = []
                        resolved_count = 0

                        for item in results['one_to_many_for_ui']:
                            cognos_col = item['cognos_col']
                            parts = cognos_col.split('.')
                            
                            if len(parts) >= 2:
                                cognos_table_norm = normalize_name(parts[-2])
                                cognos_column_norm = normalize_name(parts[-1])
                                
                                match_found = None
                                for pbi_option in item['options']:
                                    pbi_table_norm = normalize_name(pbi_option.get('table'))
                                    pbi_column_norm = normalize_name(pbi_option.get('column'))
                                    
                                    if cognos_table_norm == pbi_table_norm and cognos_column_norm == pbi_column_norm:
                                        match_found = pbi_option
                                        break
                                
                                if match_found:
                                    resolved_mappings[cognos_col] = match_found
                                    resolved_count += 1
                                else:
                                    still_ambiguous_ui.append(item)
                            else:
                                still_ambiguous_ui.append(item)

                        st.session_state.results['final_json']['mappings']['cognos_to_powerbi'].update(resolved_mappings)
                        st.session_state.results['one_to_many_for_ui'] = still_ambiguous_ui
                        st.session_state.results['one_to_many'] = pd.DataFrame([{
                            "Cognos Column": i['cognos_col'],
                            "Mapped Power BI Options": ", ".join([f"{opt.get('table')}.{opt.get('column')}" for opt in i['options']])
                        } for i in still_ambiguous_ui])

                        st.session_state.message = f"Auto-resolution complete! {resolved_count} mappings were resolved automatically. {len(still_ambiguous_ui)} mappings remain ambiguous."
                        st.rerun()

                    # --- MANUAL RESOLVE BUTTON ---
                    if col2.button("Resolve Manually", use_container_width=True):
                        st.session_state.show_manual_resolver = True

                    # --- MANUAL RESOLVER FORM (conditionally shown) ---
                    if st.session_state.show_manual_resolver and results['one_to_many_for_ui']:
                        with st.form("resolution_form"):
                            selections = {}
                            for item in results['one_to_many_for_ui']:
                                cognos_col = item['cognos_col']
                                options = item['options']
                                
                                option_labels = [f"{opt.get('table', 'N/A')}[{opt.get('column', 'N/A')}]" for opt in options]
                                formatted_cognos_col = '.'.join([f"[{part.title()}]" for part in cognos_col.split('.')])
                                
                                label_markdown = f"Select mapping for: <span style='color: #28a745; font-size: 1.1em;'>**{formatted_cognos_col}**</span>"
                                st.markdown(label_markdown, unsafe_allow_html=True)
                                
                                selected_label = st.radio(
                                    f"Options for {cognos_col}",
                                    options=option_labels,
                                    key=f"radio_{cognos_col}",
                                    label_visibility="collapsed"
                                )
                                if selected_label:
                                    selected_index = option_labels.index(selected_label)
                                    selections[cognos_col] = options[selected_index]

                            submitted = st.form_submit_button("Confirm Manual Selections")
                            if submitted:
                                st.session_state.results['final_json']['mappings']['cognos_to_powerbi'].update(selections)
                                st.session_state.results['one_to_many_for_ui'] = []
                                st.session_state.results['one_to_many'] = pd.DataFrame()
                                st.session_state.show_manual_resolver = False
                                st.session_state.message = "Manual selections confirmed and added to the final mapping."
                                st.rerun()
                else:
                    st.info("No ambiguous mappings to resolve.")

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

            # Filter out columns with JOIN_KEY for the creation UI
            unmapped_pbi_df = results['powerbi_none_mapped']
            mappable_pbi_df = unmapped_pbi_df[
                ~unmapped_pbi_df['Unmapped Power BI Column'].str.contains("JOIN_KEY", case=False, na=False)
            ]

            # Section to create new Cognos mappings for unmapped PBI columns
            with st.expander(f"📝 Create New Mappings for Unused Power BI Columns ({len(mappable_pbi_df)})", expanded=False):
                if not mappable_pbi_df.empty:
                    with st.form("create_new_mappings_form"):
                        new_cognos_mappings = {}
                        st.write("For each unmapped Power BI column, define a new Cognos column name to map it to. Clear the text box to skip a column.")

                        for index, row in mappable_pbi_df.iterrows():
                            pbi_table = row['Unmapped Power BI Table']
                            pbi_column = row['Unmapped Power BI Column']
                            pbi_full_string = f"{pbi_table}.{pbi_column}"
                            
                            suggested_cognos_name = f"[Presentation Layer].[{pbi_table}].[{pbi_column}]"
                            
                            new_name = st.text_input(
                                f"New Cognos Column for: **{pbi_table}[{pbi_column}]**",
                                value=suggested_cognos_name,
                                key=f"new_cognos_{pbi_full_string}"
                            )
                            new_cognos_mappings[pbi_full_string] = new_name

                        submitted = st.form_submit_button("Confirm New Mappings")
                        if submitted:
                            newly_mapped_pbi = set()
                            new_one_to_one_rows = []

                            for pbi_str, cognos_raw_name in new_cognos_mappings.items():
                                # Process only if user provided a non-empty/non-whitespace name
                                if cognos_raw_name and cognos_raw_name.strip():
                                    cognos_key = cognos_raw_name.replace('[', '').replace(']', '').lower()
                                    
                                    pbi_object = results['pbi_string_to_object_map'].get(pbi_str)
                                    if pbi_object:
                                        st.session_state.results['final_json']['mappings']['cognos_to_powerbi'][cognos_key] = pbi_object
                                        
                                        newly_mapped_pbi.add(pbi_str)
                                        pbi_table, pbi_column = pbi_str.split('.', 1)
                                        new_one_to_one_rows.append({
                                            "Cognos Column": cognos_key,
                                            "Power BI Table": pbi_table,
                                            "Power BI Column": pbi_column
                                        })

                            if new_one_to_one_rows:
                                new_one_to_one_df = pd.DataFrame(new_one_to_one_rows)
                                st.session_state.results['one_to_one'] = pd.concat([results['one_to_one'], new_one_to_one_df], ignore_index=True)

                                powerbi_df = st.session_state.results['powerbi_none_mapped']
                                powerbi_df['full_name'] = powerbi_df['Unmapped Power BI Table'] + '.' + powerbi_df['Unmapped Power BI Column']
                                st.session_state.results['powerbi_none_mapped'] = powerbi_df[
                                    ~powerbi_df['full_name'].isin(newly_mapped_pbi)
                                ].drop(columns=['full_name'])

                                st.session_state.message = f"{len(newly_mapped_pbi)} new mappings have been created and added to the 'One-to-One' section."
                                st.rerun()
                            else:
                                st.warning("No new mappings were created. Please provide names in the text boxes to create mappings.")
                else:
                    st.info("There are no unmapped Power BI columns to create mappings for.")
            
            # Add download button for the final JSON
            if st.session_state.results['final_json']['mappings']['cognos_to_powerbi']:
                st.header("⬇️ Download Full Mapping")

                total_mapped_count = len(st.session_state.results['final_json']['mappings']['cognos_to_powerbi'])
                st.metric(label="Total Mapped Columns", value=total_mapped_count)

                final_json_string = json.dumps(st.session_state.results['final_json'], indent=2)
                st.download_button(
                    label="Download resolved mapping as JSON",
                    data=final_json_string,
                    file_name="cognos_to_powerbi_mapping.json",
                    mime="application/json"
                )
                st.json(st.session_state.results['final_json'])

    except FileNotFoundError:
        st.error("Error: `data/column_mappings.json` not found. Please make sure the file exists in the `data` directory.")
    except json.JSONDecodeError:
        st.error("Error: The file `data/column_mappings.json` is not a valid JSON file.")
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()