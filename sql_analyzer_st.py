import streamlit as st
import pandas as pd
import sqlparse
import google.generativeai as genai
import os

# Import the analyzer from sql_analyzer.py
from src.utils.sql_analyzer import SQLLineageAnalyzer


from dotenv import load_dotenv
load_dotenv(dotenv_path='.env')

from src.constants import API_KEY

# Set up Gemini API with internal API key
# For security, best practice is to use environment variables  # Replace with your actual API key if not using env var
genai.configure(api_key=API_KEY)

def generate_dax_from_sql(sql_expression):
    try:
        model = genai.GenerativeModel('gemini-2.5-flash-preview-05-20')
        prompt = f"""
        Analyze the following SQL expression and provide:
        1. An equivalent PowerBI DAX expression for a MEASURE (properly formatted with line breaks and indentation for readability)
        2. An equivalent PowerBI DAX expression for a CALCULATED COLUMN (properly formatted with line breaks and indentation for readability)
        3. A recommendation on whether this should be implemented as a measure or calculated column in PowerBI based on its characteristics

        SQL Expression:
        ```sql
        {sql_expression}
        ```
        
        Format your response exactly like this example with no additional text:
        MEASURE: 
        CALCULATE(
            SUM(Sales[Revenue]),
            Sales[Year] = 2023
        )
        CALCULATED_COLUMN: 
        IF(
            [Price] * [Quantity] > 1000,
            "High Value",
            "Standard"
        )
        RECOMMENDATION: measure
        """
        
        response = model.generate_content(prompt)
        
        # Clean up response to remove markdown formatting
        dax_response = response.text.strip()
        
        # Extract the different sections using more sophisticated parsing
        sections = {'measure': '', 'calculated_column': '', 'recommendation': ''}
        
        # Split by sections markers
        parts = dax_response.split('MEASURE:')
        if len(parts) > 1:
            rest = parts[1]
            
            # Get CALCULATED_COLUMN section
            calc_parts = rest.split('CALCULATED_COLUMN:')
            if len(calc_parts) > 1:
                sections['measure'] = calc_parts[0].strip()
                rest = calc_parts[1]
                
                # Get RECOMMENDATION section
                rec_parts = rest.split('RECOMMENDATION:')
                if len(rec_parts) > 1:
                    sections['calculated_column'] = rec_parts[0].strip()
                    sections['recommendation'] = rec_parts[1].strip()
                else:
                    sections['calculated_column'] = rest.strip()

        # Clean up any markdown formatting in the sections
        for key in ['measure', 'calculated_column']:
            # Remove code block markers
            sections[key] = sections[key].replace('```dax', '').replace('```', '')
            
            # Remove language identifier if it appears at the beginning
            if sections[key].lstrip().startswith('dax'):
                sections[key] = sections[key].lstrip()[3:].lstrip()

            if sections[key].lstrip().startswith('DAX'):
                sections[key] = sections[key].lstrip()[3:].lstrip()
                
            # Remove any trailing backticks
            sections[key] = sections[key].rstrip('`').strip()
        
        return {
            "measure": sections['measure'],
            "calculated_column": sections['calculated_column'],
            "recommendation": sections['recommendation']
        }
    except Exception as e:
        return {
            "measure": f"Error: {str(e)}",
            "calculated_column": f"Error: {str(e)}",
            "recommendation": "error"
        }

def main():
    st.set_page_config(
        page_title="SQL Lineage Analyzer",
        page_icon="📊",
        layout="wide"
    )
    
    # Initialize session state variables
    if 'sql_query' not in st.session_state:
        st.session_state['sql_query'] = ""
    if 'lineage_data' not in st.session_state:
        st.session_state['lineage_data'] = None
    if 'all_types' not in st.session_state:
        st.session_state['all_types'] = []
    if 'dax_expressions' not in st.session_state:
        st.session_state['dax_expressions'] = {}
    
    st.title("SQL Lineage Analyzer")
    st.markdown("""
    This tool analyzes SQL queries to understand column lineage and can generate equivalent DAX expressions.
    """)
    
    # Main content area
    col1, col2 = st.columns([4, 1])
    
    with col1:
        # Input area for SQL query with session state to preserve content
        sql_query = st.text_area("Enter your SQL query:", 
                                value=st.session_state.get('sql_query', ""),
                                height=300)
        st.session_state['sql_query'] = sql_query
    
    with col2:
        st.write("### Actions")
        analyze_button = st.button("Analyze Query", use_container_width=True)
        clear_button = st.button("Clear Query", use_container_width=True)
        
        if clear_button:
            st.session_state['sql_query'] = ""
            st.session_state['lineage_data'] = None
            st.session_state['all_types'] = []
            st.session_state['dax_expressions'] = {}
            st.rerun()
    
    # When analyze button is clicked or we already have data
    if analyze_button and sql_query.strip():
        try:
            # Run the analysis with fixed dialect (snowflake)
            with st.spinner("Analyzing query..."):
                analyzer = SQLLineageAnalyzer(sql_query, dialect="snowflake")
                st.session_state['lineage_data'] = analyzer.analyze()
                
                # Get unique types for filtering
                if st.session_state['lineage_data']:
                    df = pd.DataFrame(st.session_state['lineage_data'])
                    st.session_state['all_types'] = sorted(df['type'].unique().tolist())
        except Exception as e:
            st.error(f"Error analyzing query: {str(e)}")
            st.exception(e)
    
    # Display results if we have data (either from button click or from session state)
    if st.session_state['lineage_data']:
        st.subheader("Lineage Analysis Results")
        
        # Convert to DataFrame for display
        df = pd.DataFrame(st.session_state['lineage_data'])
        
        # Create tabs for different views
        tab1, tab2, tab3 = st.tabs(["Table View", "Detail View", "Raw JSON"])
        
        with tab1:
            # Add type filter - always show even for one option
            selected_types_tab1 = st.multiselect(
                "Filter by type:",
                options=st.session_state['all_types'],
                default=st.session_state['all_types'],
                key="filter_types_tab1"
            )
            
            # Only filter if selections are made
            if selected_types_tab1:
                filtered_df = df[df['type'].isin(selected_types_tab1)]
            else:
                filtered_df = df  # Show all if nothing selected
            
            # Display filtered dataframe
            st.dataframe(filtered_df, use_container_width=True)
            
            # Download option for filtered data
            csv = filtered_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name="lineage_analysis.csv",
                mime="text/csv"
            )
        
        with tab2:
            # Add type filter - always show even for one option
            selected_types_tab2 = st.multiselect(
                "Filter by type:",
                options=st.session_state['all_types'],
                default=st.session_state['all_types'],
                key="filter_types_tab2"
            )
            
            # Only filter if selections are made
            if selected_types_tab2:
                filtered_items = [item for item in st.session_state['lineage_data'] if item['type'] in selected_types_tab2]
            else:
                filtered_items = st.session_state['lineage_data']  # Show all if nothing selected
            
            for i, item in enumerate(filtered_items):
                with st.expander(f"Column: {item['column']} ({item['type']})"):
                    st.write("**Type:** ", item['type'])
                    
                    if item['type'] == 'expression' and item['final_expression']:
                        # Format the SQL expression nicely
                        formatted_expr = sqlparse.format(
                            item['final_expression'],
                            reindent=True,
                            keyword_case='upper',
                            indent_width=2
                        )
                        st.write("**SQL Expression:**")
                        st.code(formatted_expr, language="sql")
                                                
                        # Add DAX generation feature
                        item_id = f"{item['column']}_{i}"

                        if st.button(f"Generate DAX", key=f"dax_btn_{item_id}"):
                            with st.spinner("Generating DAX..."):
                                dax_results = generate_dax_from_sql(item['final_expression'])
                                st.session_state['dax_expressions'][item_id] = dax_results

                        # Display DAX if available
                        if item_id in st.session_state['dax_expressions']:
                            dax_results = st.session_state['dax_expressions'][item_id]

                            # Display recommendation
                            recommendation = dax_results.get("recommendation", "").lower()
                            if recommendation == "measure":
                                st.info("💡 **Recommendation:** **MEASURE**")
                            elif recommendation == "calculated_column" or recommendation == "calculated column":
                                st.info("💡 **Recommendation:** **CALCULATED COLUMN**")

                            # Display measure expression
                            st.write("**DAX Measure:**")
                            st.code(dax_results.get("measure", ""), language="")

                            # Display calculated column expression
                            st.write("**DAX Calculated Column:**")
                            st.code(dax_results.get("calculated_column", ""), language="")
                    
                    elif item['type'] == 'expression':
                        st.code("No expression available", language="sql")
                        
                    st.write("**Base columns:**")
                    for col in item['base_columns']:
                        st.write(f"- `{col}`")
        
        with tab3:
            st.json(st.session_state['lineage_data'])
    
    elif analyze_button and not sql_query.strip():
        st.warning("Please enter a SQL query")

if __name__ == "__main__":
    main()