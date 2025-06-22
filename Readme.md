# Cognos to Power BI Report Converter

This is a Streamlit web application designed to accelerate the migration of reports from IBM Cognos to Microsoft Power BI. It parses a Cognos report's XML specification, maps its data items to a target Power BI semantic model, and generates a complete Power BI Project (`.pbip`) file structure, ready to be opened in Power BI Desktop.

## Key Features

-   **XML Parsing**: Ingests the raw XML from a Cognos report definition.
-   **Automated Mapping**: Maps Cognos query subjects and data items to corresponding Power BI tables and columns using a configurable JSON mapping file.
-   **Interactive UI**: A user-friendly web interface to guide the conversion process.
-   **Ambiguity Resolution**: Provides a UI for users to resolve cases where a single Cognos item could map to multiple Power BI fields.
-   **AI-Powered DAX Generation**: Leverages a Large Language Model (LLM) to automatically convert Cognos business logic and calculations into DAX measures.
-   **Visual Configuration**: Allows users to define the structure of the target Power BI report, including pages and visual types (Tables, Matrices), and assign fields to them.
-   **Dynamic Report Generation**: Creates all the necessary JSON files for a `.pbip` project, including the report layout, data model connections, and settings.
-   **Downloadable Artifact**: Packages the entire generated Power BI project into a single, downloadable `.zip` archive.

## How It Works: The Conversion Workflow

The application follows a multi-step process to convert a report:

1.  **Paste XML**: The user pastes the Cognos report XML content into a text area.
2.  **Analysis & Mapping**:
    -   The app parses the XML to identify pages, queries, visuals, and data items (rows, columns, measures, filters).
    -   It uses `config/column_mappings.json` to find the corresponding Power BI table and column for each Cognos data item.
    -   A status table is displayed showing which items were successfully mapped (`✅`) and which were not (`❌`).
3.  **Resolve Ambiguities**: If any Cognos item has multiple potential mappings in the JSON file, the user is presented with dropdowns to select the correct one.
4.  **Configure Visuals**:
    -   The user can configure each visual from the original report.
    -   They can select the target visual type (e.g., Matrix, Table) and drag-and-drop the mapped fields into the appropriate wells (Rows, Columns, Values).
5.  **Generate DAX Measures**: The user can click a button to send the business expressions for measures to an AI service, which returns generated DAX code. This DAX is automatically incorporated into the report configuration.
6.  **Generate & Download Report**:
    -   With a final click, the application compiles all the user's configurations into an in-memory representation of a `config.yaml` file.
    -   It then calls the report generation module, which uses this configuration along with templates (`theme.json`, `semantic.json`) to create the content for all the files in the `.pbip` project structure.
    -   These files are zipped in-memory, and a download button appears for the user to save the complete project archive.