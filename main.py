import json
from typing import Dict, List, Any, Optional, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.expressions import Expression


class SQLLineageAnalyzer:
    def __init__(self, sql_query: str, dialect: str = "snowflake"):
        self.sql_query = sql_query
        self.dialect = dialect
        self.ast = sqlglot.parse_one(sql_query, read=self.dialect)
        self.cte_definitions: Dict[str, exp.Select] = {}
        self._extract_ctes()
        self.final_select = self._get_final_select()

    def _extract_ctes(self):
        """Extract all CTE definitions from the query."""
        if not self.ast: return
        for with_expr in self.ast.find_all(exp.With):
            for cte in with_expr.expressions:
                if isinstance(cte, exp.CTE):
                    self.cte_definitions[cte.alias.upper()] = cte.this

    def _get_final_select(self) -> Optional[exp.Select]:
        """Find the final SELECT statement in the AST."""
        if not self.ast: return None
        if isinstance(self.ast, exp.With): return self.ast.this
        if isinstance(self.ast, exp.Select): return self.ast
        selects = list(self.ast.find_all(exp.Select))
        return selects[-1] if selects else None

    def analyze(self) -> List[Dict[str, Any]]:
        """Analyze the SQL query and return a concise, fully resolved lineage."""
        results = []
        if not self.final_select: return []

        for col_expr in self.final_select.expressions:
            col_name = col_expr.alias_or_name

            final_expression_ast = self._resolve_expression_fully(col_expr, self.final_select, set())

            # *** THE FIX IS HERE ***
            # A column is direct if its fully resolved form is still just a single column.
            # The name doesn't matter.
            is_direct = isinstance(final_expression_ast, exp.Column)

            final_expression_sql = final_expression_ast.sql(dialect=self.dialect)

            # Base columns are any columns left in the AST after full resolution.
            base_columns = {
                # Use sqlglot to render the final column name correctly
                c.sql(dialect=self.dialect)
                for c in final_expression_ast.find_all(exp.Column)
            }

            results.append({
                "column": col_name,
                "type": "base" if is_direct else "expression",
                "final_expression": None if is_direct else final_expression_sql,
                "base_columns": sorted(list(base_columns))
            })
        return results

    def _resolve_expression_fully(self, expression: Expression, scope: exp.Select, visited: Set) -> Expression:
        """
        Uses sqlglot's transform to recursively replace columns with their definitions.
        """

        def _resolver(node):
            if isinstance(node, exp.Column):
                return self._trace_and_replace_column(node, scope, visited.copy())
            return node

        resolved_ast = expression.transform(_resolver, copy=True)  # Use copy=True for safety

        if isinstance(resolved_ast, exp.Alias):
            return resolved_ast.this
        return resolved_ast

    def _trace_and_replace_column(self, column: exp.Column, scope: exp.Select, visited: Set) -> Expression:
        """
        Traces a column to its definition and returns the corresponding expression AST.
        """
        column_name = column.name.upper()
        table_alias = column.table.upper() if column.table else None

        trace_id = (id(scope), table_alias, column_name)
        if trace_id in visited:
            return column  # Cycle detected

        visited.add(trace_id)

        source = self._find_source_for_alias(table_alias, scope)
        if not source:
            return column  # Cannot resolve, treat as base

        source_type, source_name, source_node = source

        if source_type == "table":
            # Fully qualify the column from the base table and return it.
            base_table = source_node
            column.set('table', exp.Identifier(this=base_table.name))
            if base_table.db:
                column.set('db', exp.Identifier(this=base_table.db))
                if base_table.catalog:
                    column.set('catalog', exp.Identifier(this=base_table.catalog))
            return column

        if source_type in ["cte", "subquery"]:
            for sub_expr in source_node.expressions:
                if sub_expr.alias_or_name.upper() == column_name:
                    # Recursively resolve this new expression in its own scope.
                    return self._resolve_expression_fully(sub_expr, source_node, visited)

        return column  # Fallback

    def _find_source_for_alias(
            self, alias: Optional[str], scope: exp.Select
    ) -> Optional[Tuple[str, str, Expression]]:
        """Finds what a table alias refers to."""
        sources = scope.find_all(exp.From, exp.Join)
        for source in sources:
            source_item = source.this
            source_alias = source_item.alias_or_name.upper()

            # If an alias is specified, we must have an exact match.
            if alias and alias != source_alias:
                continue

            # If no alias is specified (ambiguous column), we can't reliably resolve,
            # but this block is a good starting point for a simple case.
            if not alias:
                # In a more complex system, you'd check if the column exists in this source.
                # For now, we assume if no alias is given, we can't be sure.
                pass

            if isinstance(source_item, exp.Table):
                table_name = source_item.name.upper()
                if table_name in self.cte_definitions:
                    return "cte", table_name, self.cte_definitions[table_name]
                else:
                    return "table", table_name, source_item

            if isinstance(source_item, (exp.Subquery, exp.CTE)):
                return "subquery", source_alias, source_item.this
        return None


def analyze_sql_lineage(sql_query: str) -> List[Dict[str, Any]]:
    analyzer = SQLLineageAnalyzer(sql_query)
    return analyzer.analyze()


if __name__ == "__main__":
    # Your full query here... I'm using a simplified version that demonstrates the fix
    # on the exact column you pointed out.
    full_query = """
  WITH 
"SKU" AS 
    (
    SELECT
        "D1"."C0" AS "Plant_DESC", 
        "D1"."C1" AS "Plant_ID", 
        "D1"."C2" AS "Material_ID", 
        "D1"."C3" AS "Material_Part_ID", 
        "D1"."C4" AS "Week_Year", 
        "D1"."C5" AS "Year_ID", 
        "D1"."C6" AS "Week_ID", 
        "D1"."C7" AS "Week_Month_DT", 
        "D1"."C8" AS "Month_DESC", 
        SUM("D1"."C16") AS "Prod_QTY__week_", 
        SUM("D1"."C17") AS "Prod_NSDS_QTY__week_", 
        SUM("D1"."C18") AS "Planned_QTY__1st_of_the_Week_", 
        CASE 
            WHEN SUM("D1"."C19") < SUM("D1"."C18") THEN CAST(SUM("D1"."C19") AS DOUBLE PRECISION) / NULLIF(SUM("D1"."C18"), 0)
            ELSE 1
        END AS "Variance____Non_NSDS___1st_of_the_Week_", 
        CASE 
            WHEN 
                SUM("D1"."C16") <> SUM("D1"."C17") AND
                MIN("D1"."C20") = '1'
                THEN
                    'NS/OS'
            ELSE ''
        END AS "NSDS_OE_or_SCRAP__RE_", 
        CASE 
            WHEN 
                SUM("D1"."C16") <> SUM("D1"."C17") AND
                MIN("D1"."C20") = '0'
                THEN
                    'NS/OS'
            ELSE ''
        END AS "NSDS_OE_or_SCRAP__OE_", 
        SUM("D1"."C21") AS "Threshold_classification___ok", 
        COUNT("D1"."C2") AS "SKU_count", 
        "D1"."C9" AS "Hierarchy_Level_2_CD", 
        "D1"."C10" AS "Hierarchy_Level_2_DESC", 
        "D1"."C11" AS "TMS_L2_By_Market", 
        "D1"."C12" AS "TMS_order", 
        "D1"."C13" AS "RIM", 
        "D1"."C14" AS "A_B", 
        CAST(SUM("D1"."C16") - SUM("D1"."C18") AS DOUBLE PRECISION) / NULLIF(SUM("D1"."C18"), 0) AS "Variance__", 
        "D1"."C15" AS "PBU_order"
    FROM
        (
        SELECT
            "FACILITY_CURR_6"."FACILITY_DESC" AS "C0", 
            "FACILITY_CURR_6"."FACILITY_ID" AS "C1", 
            "MFG_PROD_HIST"."MATL_ID" AS "C2", 
            "MFG_PROD_HIST"."MATL_PART_ID" AS "C3", 
            EXTRACT(YEAR FROM "CALENDAR_ISO"."ISO_WK_OVRLP_MTH_DT") AS "C4", 
            "CALENDAR_ISO"."YEAR_OF_CALENDAR" AS "C5", 
            "CALENDAR_ISO"."WEEK_OF_YEAR_ISO" AS "C6", 
            "CALENDAR_ISO"."ISO_WK_OVRLP_MTH_DT" AS "C7", 
            "CALENDAR_ISO"."MNTH_DESCR" AS "C8", 
            "MATL_MART"."HIER_LVL_2_CD" AS "C9", 
            "MATL_MART"."HIER_LVL_2_DESC" AS "C10", 
            "MATL_MART"."TMS_LVL_2_BY_MKT_SEGMNT_DESC" AS "C11", 
            CASE 
                WHEN "MATL_MART"."TMS_LVL_2_BY_MKT_SEGMNT_DESC" = 'PERFORMANCE & SUV' THEN '01'
                WHEN "MATL_MART"."TMS_LVL_2_BY_MKT_SEGMNT_DESC" = 'COMMUTER CORE' THEN '02'
                WHEN "MATL_MART"."TMS_LVL_2_BY_MKT_SEGMNT_DESC" = 'COMMUTER STANDARD' THEN '03'
                WHEN "MATL_MART"."TMS_LVL_2_BY_MKT_SEGMNT_DESC" = 'LIGHT TRUCK' THEN '04'
                WHEN "MATL_MART"."TMS_LVL_2_BY_MKT_SEGMNT_DESC" = 'GROW PROFITABLE TMS' THEN '05'
                WHEN "MATL_MART"."TMS_LVL_2_BY_MKT_SEGMNT_DESC" = 'COMPETE PROFITABLE TMS' THEN '06'
                ELSE "MATL_MART"."TMS_LVL_2_BY_MKT_SEGMNT_DESC"
            END AS "C12", 
            "MATL_MART"."RIM_DIAM_QTY" AS "C13", 
            CASE 
                WHEN "MATL_MART"."RIM_DIAM_QTY" >= '17' THEN 'A'
                ELSE 'B'
            END AS "C14", 
            CASE 
                WHEN "MATL_MART"."HIER_LVL_2_CD" = '10' THEN '01'
                WHEN "MATL_MART"."HIER_LVL_2_CD" = '40' THEN '02'
                WHEN "MATL_MART"."HIER_LVL_2_CD" = '80' THEN '03'
                ELSE "MATL_MART"."HIER_LVL_2_CD"
            END AS "C15", 
            "MFG_PROD_HIST"."WK_PROD_QTY" AS "C16", 
            "MFG_PROD_HIST"."WK_PROD_NSDS_QTY" AS "C17", 
            "MFG_PROD_HIST"."WK_PLN_WK_SNAP_QTY" AS "C18", 
            ABS("MFG_PROD_HIST"."WK_PLN_WK_SNAP_QTY" - "MFG_PROD_HIST"."WK_PROD_QTY") AS "C19", 
            "MATL_MART"."TIRE_TYP_IND" AS "C20", 
            CASE 
                WHEN 
                    CASE 
                        WHEN ABS("MFG_PROD_HIST"."WK_PLN_WK_SNAP_QTY" - "MFG_PROD_HIST"."WK_PROD_QTY") < "MFG_PROD_HIST"."WK_PLN_WK_SNAP_QTY" THEN CAST(ABS("MFG_PROD_HIST"."WK_PLN_WK_SNAP_QTY" - "MFG_PROD_HIST"."WK_PROD_QTY") AS DOUBLE PRECISION) / NULLIF("MFG_PROD_HIST"."WK_PLN_WK_SNAP_QTY", 0)
                        ELSE 1
                    END <= '0.1' OR
                    CAST("MFG_PROD_HIST"."WK_PROD_QTY" - "MFG_PROD_HIST"."WK_PLN_WK_SNAP_QTY" AS DOUBLE PRECISION) / NULLIF("MFG_PROD_HIST"."WK_PLN_WK_SNAP_QTY", 0) >= '-0.1' AND
                    CASE 
                        WHEN 
                            "MFG_PROD_HIST"."WK_PROD_QTY" <> "MFG_PROD_HIST"."WK_PROD_NSDS_QTY" AND
                            "MATL_MART"."TIRE_TYP_IND" = '0'
                            THEN
                                'NS/OS'
                        ELSE ''
                    END = 'NS/OS' OR
                    CAST("MFG_PROD_HIST"."WK_PROD_QTY" - "MFG_PROD_HIST"."WK_PLN_WK_SNAP_QTY" AS DOUBLE PRECISION) / NULLIF("MFG_PROD_HIST"."WK_PLN_WK_SNAP_QTY", 0) <= '0.1' AND
                    CASE 
                        WHEN 
                            "MFG_PROD_HIST"."WK_PROD_QTY" <> "MFG_PROD_HIST"."WK_PROD_NSDS_QTY" AND
                            "MATL_MART"."TIRE_TYP_IND" = '1'
                            THEN
                                'NS/OS'
                        ELSE ''
                    END = 'NS/OS'
                    THEN
                        '1'
                ELSE '0'
            END AS "C21"
        FROM
            "PROD"."EU_BI_VWS"."MFG_PROD_HIST" "MFG_PROD_HIST"
                INNER JOIN "PROD"."EU_BI_VWS"."CALENDAR_ISO" "CALENDAR_ISO"
                ON "MFG_PROD_HIST"."PROD_DT" = "CALENDAR_ISO"."CALENDAR_DATE"
                    LEFT OUTER JOIN "PROD"."EU_BI_VWS"."FACILITY_CURR_6" "FACILITY_CURR_6"
                    ON "FACILITY_CURR_6"."FACILITY_ID" = "MFG_PROD_HIST"."FACILITY_ID"
                        LEFT OUTER JOIN "PROD"."EU_BI_VWS"."MATL_MART" "MATL_MART"
                        ON "MFG_PROD_HIST"."MATL_ID" = "MATL_MART"."MATL_ID" 
        WHERE 
            "FACILITY_CURR_6"."FACILITY_ID" <> 'LUGI' AND
            "MFG_PROD_HIST"."EXP_DT" = CAST('5555-12-31' AS DATE) AND
            "MFG_PROD_HIST"."WK_PLN_WK_SNAP_QTY" <> 0 AND
            "MFG_PROD_HIST"."MATL_PART_ID" IN ( 
                'sc' ) AND
            "MFG_PROD_HIST"."PROD_DT" BETWEEN to_date ('2025-04-28', 'YYYY-MM-DD') AND to_date ('2025-06-08', 'YYYY-MM-DD') AND
            "FACILITY_CURR_6"."FACILITY_ID" = '0001'
        ) "D1" 
    GROUP BY 
        "D1"."C0", 
        "D1"."C1", 
        "D1"."C2", 
        "D1"."C3", 
        "D1"."C4", 
        "D1"."C5", 
        "D1"."C6", 
        "D1"."C7", 
        "D1"."C8", 
        "D1"."C9", 
        "D1"."C10", 
        "D1"."C11", 
        "D1"."C12", 
        "D1"."C13", 
        "D1"."C14", 
        "D1"."C15"
    )
SELECT
    "SKU"."Plant_DESC" AS "Plant_DESC", 
    "SKU"."Week_ID" AS "Week_ID", 
    SUM("SKU"."Threshold_classification___ok") / NULLIF(SUM("SKU"."SKU_count"), 0) AS "ILD", 
    "SKU"."Hierarchy_Level_2_DESC" AS "Hierarchy_Level_2_DESC", 
    "SKU"."Hierarchy_Level_2_CD" AS "Hierarchy_Level_2_CD", 
    "SKU"."Week_Year" AS "Week_Year", 
    "SKU"."Year_ID" AS "Year_ID", 
    "SKU"."Week_Month_DT" AS "Week_Month_DT", 
    "SKU"."Month_DESC" AS "Month_DESC", 
    "SKU"."Plant_ID" AS "Plant_ID", 
    "SKU"."Material_Part_ID" AS "Material_Part_ID", 
    SUM("SKU"."Threshold_classification___ok") AS "Threshold_classification___ok", 
    SUM("SKU"."SKU_count") AS "SKU_count", 
    "SKU"."A_B" AS "A_B", 
    "SKU"."TMS_L2_By_Market" AS "TMS_L2_By_Market", 
    "SKU"."TMS_order" AS "TMS_order", 
    "SKU"."PBU_order" AS "PBU_order"
FROM
    "SKU" 
WHERE 
    "SKU"."Hierarchy_Level_2_CD" IN ( 
        '10', 
        '20', 
        '30', 
        '40', 
        '80' ) 
GROUP BY 
    "SKU"."Plant_DESC", 
    "SKU"."Week_ID", 
    "SKU"."Hierarchy_Level_2_DESC", 
    "SKU"."Hierarchy_Level_2_CD", 
    "SKU"."Week_Year", 
    "SKU"."Year_ID", 
    "SKU"."Week_Month_DT", 
    "SKU"."Month_DESC", 
    "SKU"."Plant_ID", 
    "SKU"."Material_Part_ID", 
    "SKU"."A_B", 
    "SKU"."TMS_L2_By_Market", 
    "SKU"."TMS_order", 
    "SKU"."PBU_order"
    """

    print("Query analysis with corrected 'direct' type detection:")
    lineage_data = analyze_sql_lineage(full_query)
    print(json.dumps(lineage_data, indent=2))