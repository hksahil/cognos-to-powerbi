import streamlit as st
import json
import pandas as pd
import sqlparse

# Import the analyzer from main.py
from main import SQLLineageAnalyzer

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
    
    st.title("SQL Lineage Analyzer")
    st.markdown("""
    This tool analyzes SQL queries to understand column lineage - how columns in the result set 
    relate to source tables and expressions.
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
            
            for item in filtered_items:
                with st.expander(f"{item['column']} ({item['type']})"):
                    st.write("**Type:** ", item['type'])
                    if item['type'] == 'expression':
                        # Format the SQL expression nicely
                        if item['final_expression']:
                            formatted_expr = sqlparse.format(
                                item['final_expression'],
                                reindent=True,
                                keyword_case='upper',
                                indent_width=2
                            )
                            st.code(formatted_expr, language="sql")
                        else:
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