import streamlit as st
import json
import pandas as pd
from datetime import datetime

def main():
    st.set_page_config(
        page_title="Power BI Model SQL Extractor",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("Power BI Model SQL Extractor")
    st.markdown("""
    This tool helps you create a structured JSON file containing table definitions and SQL queries for Power BI models.
    """)
    
    # Initialize session state for tables if not present
    if 'tables' not in st.session_state:
        st.session_state.tables = []
    
    if 'next_id' not in st.session_state:
        st.session_state.next_id = 0
        
    # Function to add a new table entry
    def add_table():
        st.session_state.tables.append({
            'id': st.session_state.next_id,
            'name': '',
            'sql': ''
        })
        st.session_state.next_id += 1
    
    # Function to remove a table
    def remove_table(idx):
        st.session_state.tables.pop(idx)
    
    # Model name input
    model_name = st.text_input("Model Name", 
                               help="Enter the name of your Power BI model")
    
    # Table inputs
    st.subheader("Tables and SQL Queries")
    
    # Button to add new table
    if st.button("Add Table"):
        add_table()
    
    # If no tables yet, add one by default
    if not st.session_state.tables:
        add_table()
    
    # Display all table inputs
    tables_to_remove = []
    for i, table in enumerate(st.session_state.tables):
        with st.container():
            col1, col2, col3 = st.columns([3, 10, 1])
            
            with col1:
                st.session_state.tables[i]['name'] = st.text_input(
                    "Table Name", 
                    value=table['name'],
                    key=f"table_name_{table['id']}",
                    placeholder="Enter table name"
                )
                
            with col3:
                if st.button("🗑️", key=f"remove_{table['id']}"):
                    tables_to_remove.append(i)
            
            st.session_state.tables[i]['sql'] = st.text_area(
                "SQL Query", 
                value=table['sql'],
                key=f"sql_query_{table['id']}",
                height=150,
                placeholder="Enter SQL query for this table"
            )
            
            st.divider()
    
    # Remove any tables marked for removal
    for idx in reversed(tables_to_remove):
        remove_table(idx)
    
    # Preview and download section
    st.subheader("Generate JSON")
    
    # Validate input before generating
    is_valid = True
    validation_message = ""
    
    if not model_name:
        is_valid = False
        validation_message = "Please enter a model name."
    
    if not any(table['name'] for table in st.session_state.tables):
        is_valid = False
        validation_message = "Please enter at least one table name."
    
    for table in st.session_state.tables:
        if table['name'] and not table['sql']:
            is_valid = False
            validation_message = f"Table '{table['name']}' has no SQL query."
    
    # Display validation message if any
    if not is_valid:
        st.warning(validation_message)
    
    # Generate JSON and provide download if valid
    if is_valid:
        # Create structured data for JSON
        model_data = {
            "name": model_name,
            "generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "tables": [
                {
                    "name": table['name'],
                    "sql": table['sql']
                }
                for table in st.session_state.tables
                if table['name'] and table['sql']  # Only include complete entries
            ]
        }
        
        # Show preview
        with st.expander("Preview JSON", expanded=True):
            st.json(model_data)
        
        # Create download button
        json_string = json.dumps(model_data, indent=2)
        filename = f"{model_name.replace(' ', '_')}_model_sql.json"
        
        st.download_button(
            label="Download JSON",
            data=json_string,
            file_name=filename,
            mime="application/json",
            key="download_button",
            use_container_width=True
        )

if __name__ == "__main__":
    main()