import google.generativeai as genai
import json

from src.constants import API_KEY

genai.configure(api_key=API_KEY)


def generate_dax_for_measure(pbi_column_expression, aggregation_type):
    """
    Generates a DAX measure using AI based on a Power BI column and aggregation type.

    Args:
        pbi_column_expression (str): The Power BI column reference (e.g., "'Sales'[Amount]").
        aggregation_type (str): The desired aggregation ('total', 'average', 'count', etc.).

    Returns:
        dict: A dictionary containing the generated 'measure' and 'dataType'.
    """
    # Map user-friendly aggregation to DAX function
    agg_map = {
        'total': 'SUM',
        'average': 'AVERAGE',
        'count': 'COUNT',
        'distinct count': 'DISTINCTCOUNT',
        'maximum': 'MAX',
        'minimum': 'MIN'
    }

    data_type_options = [
            "text", "whole number", "decimal number", "date/time", 
            "date", "time", "true/false", "fixed decimal number", "binary"
        ]
    
    dax_function = agg_map.get(aggregation_type.lower(), 'SUM') # Default to SUM

    model = genai.GenerativeModel('gemini-2.5-flash-preview-05-20')

    
    prompt = f"""
    Given the following information:
    - Power BI Column: {pbi_column_expression}
    - Desired Aggregation: {dax_function}

    Generate a DAX measure formula for this calculation.
    The measure name should be descriptive, like "{aggregation_type.capitalize()} of {pbi_column_expression.split('[')[1].replace(']', '')}".
    
    Return the result as a single, minified JSON object with two keys:
    1. "measure": An equivalent PowerBI DAX expression for a MEASURE (properly formatted with line breaks and indentation for readability, don't give name to the measure, only show expression)
    2. "dataType": A suitable Power BI DATA TYPE for the MEASURE. Choose one from the following list: {', '.join(data_type_options)}.

    Example Input:
    - Power BI Column: 'Sales'[Sales Amount]
    - Desired Aggregation: SUM

    Example Output:
    {{"measure":"SUM('Sales'[Sales Amount])","dataType":"currency"}}
    """
    
    try:
        response = model.generate_content(prompt)
        # Clean up the response to extract the JSON
        cleaned_response = response.text.strip().replace("```json", "").replace("```", "")
        result = json.loads(cleaned_response)

        print(result)
        return result
    except Exception as e:
        print(f"Error generating or parsing DAX from AI: {e}")
        return {"measure": f"Error: Could not generate DAX for {dax_function}({pbi_column_expression})", "dataType": "text"}
