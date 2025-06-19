import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime

# Import the SQLLineageAnalyzer from your existing project
from sql_analyzer import SQLLineageAnalyzer

class PowerBIColumnMapper:
    """Map PowerBI columns to their source database columns."""
    
    def __init__(self, model_json_path: str):
        """Initialize with the path to the model JSON file."""
        self.model_json_path = model_json_path
        self.model_data = self._load_model_file()
        self.mappings = {
            "db_to_powerbi": {},  # Database column -> PowerBI column
            "powerbi_to_db": {},   # PowerBI column -> Database column
            "expression_to_powerbi": {}  # <-- new mapping
        }
    
    def _load_model_file(self) -> Dict:
        """Load the model JSON file."""
        try:
            with open(self.model_json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"Loaded model file with {len(data.get('tables', []))} tables")
            return data
        except Exception as e:
            print(f"Error loading model file: {str(e)}")
            return {"tables": []}
    
    def process_all_tables(self, dialect: str = "snowflake") -> Dict:
        """Process all tables in the model and create mappings."""
        total_tables = len(self.model_data.get('tables', []))
        processed = 0
        failed = 0
        columns_mapped = 0
        
        print(f"Starting to process {total_tables} tables...")
        
        for table in self.model_data.get('tables', []):
            table_name = table.get('name')
            sql_query = table.get('sql')
            
            if not table_name or not sql_query:
                print(f"Skipping table with missing name or SQL")
                failed += 1
                continue
                
            try:
                print(f"Processing table: {table_name}")
                columns_mapped += self._process_table(table_name, sql_query, dialect)
                processed += 1
            except Exception as e:
                print(f"Error processing table '{table_name}': {str(e)}")
                failed += 1
        
        print(f"Processed {processed} tables successfully, {failed} failed")
        print(f"Created mappings for {columns_mapped} columns")
        return self.mappings
    
    def _process_table(self, table_name: str, sql_query: str, dialect: str) -> int:
        """Process a single table and update mappings."""
        try:
            # Analyze the SQL query
            analyzer = SQLLineageAnalyzer(sql_query, dialect=dialect)
            lineage_results = analyzer.analyze()
            
            columns_mapped = 0
            
            # Process each column from the lineage results
            for item in lineage_results:
                column_name = item['item']
                column_type = item['type']
                base_columns = item['base_columns']

                powerbi_column = f"{table_name}.{column_name}"
                
                # Only proceed if this is a direct column (not an expression)
                if column_type == "base":

                    # For each base column
                    for db_column in base_columns:
                        # Clean up the column name
                        clean_db_column = db_column.replace('"', '')
                        
                        # Add to database -> PowerBI mapping
                        if clean_db_column not in self.mappings["db_to_powerbi"]:
                            self.mappings["db_to_powerbi"][clean_db_column] = []
                        self.mappings["db_to_powerbi"][clean_db_column].append({
                            "powerbi_column": powerbi_column,
                            "table": table_name,
                            "column": column_name
                        })
                        
                        # Add to PowerBI -> database mapping
                        if powerbi_column not in self.mappings["powerbi_to_db"]:
                            self.mappings["powerbi_to_db"][powerbi_column] = []
                        self.mappings["powerbi_to_db"][powerbi_column].append({
                            "db_column": clean_db_column
                        })
                        
                elif column_type == "expression":
                    final_expression = item.get("final_expression")
                    if not final_expression:
                        continue
                    if final_expression not in self.mappings["expression_to_powerbi"]:
                        self.mappings["expression_to_powerbi"][final_expression] = []
                    self.mappings["expression_to_powerbi"][final_expression].append({
                        "powerbi_column": powerbi_column,
                        "table": table_name,
                        "column": column_name
                    })
                    columns_mapped += 1
            
            return columns_mapped
            
        except Exception as e:
            print(f"Error analyzing SQL for table '{table_name}': {str(e)}")
            return 0
    
    def save_mappings(self, output_file: str) -> None:
        """Save the mappings to a JSON file."""
        output_data = {
            "model_name": self.model_data.get('name', 'Unknown Model'),
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mappings": self.mappings
        }
        
        try:
            # Ensure directory exists
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write the file
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2)
                
            print(f"Saved mappings to {output_file}")
        except Exception as e:
            print(f"Error saving mappings: {str(e)}")
    
    def generate_excel_report(self, output_excel: str = None) -> pd.DataFrame:
        """Generate an Excel report with the mappings."""
        rows = []
        
        for db_col, powerbi_cols in self.mappings["db_to_powerbi"].items():
            for powerbi_info in powerbi_cols:
                rows.append({
                    "database_column": db_col,
                    "powerbi_column": powerbi_info["powerbi_column"],
                    "powerbi_table": powerbi_info["table"],
                    "powerbi_column_name": powerbi_info["column"]
                })
        
        # Create DataFrame
        df = pd.DataFrame(rows)
        
        # Save to Excel if path provided
        if output_excel:
            try:
                # Ensure directory exists
                output_path = Path(output_excel)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Write the Excel file
                df.to_excel(output_path, index=False)
                print(f"Saved Excel report to {output_excel}")
            except Exception as e:
                print(f"Error saving Excel report: {str(e)}")
        
        return df


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate PowerBI column to database column mappings")
    parser.add_argument("input_file", help="Path to JSON file with tables and SQL queries")
    parser.add_argument("--output-json", "-o", default="../dump/column_mappings.json", help="Output JSON file path")
    parser.add_argument("--output-excel", "-e", default="../dump/column_mappings.xlsx", help="Output Excel file path")
    parser.add_argument("--dialect", "-d", default="snowflake", help="SQL dialect")
    
    args = parser.parse_args()
    
    # Create mapper and process tables
    mapper = PowerBIColumnMapper(args.input_file)
    mapper.process_all_tables(dialect=args.dialect)
    
    # Save outputs
    mapper.save_mappings(args.output_json)
    mapper.generate_excel_report(args.output_excel)


if __name__ == "__main__":
    main()