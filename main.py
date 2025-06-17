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
        if not self.ast: return
        for with_expr in self.ast.find_all(exp.With):
            for cte in with_expr.expressions:
                if isinstance(cte, exp.CTE):
                    self.cte_definitions[cte.alias.upper()] = cte.this

    def _get_final_select(self) -> Optional[exp.Select]:
        if not self.ast: return None
        if isinstance(self.ast, exp.With): return self.ast.this
        if isinstance(self.ast, exp.Select): return self.ast
        selects = list(self.ast.find_all(exp.Select))
        return selects[-1] if selects else None

    # --- CORRECTED METHOD ---
    def analyze(self) -> List[Dict[str, Any]]:
        """
        Analyze the SQL query and return a concise, fully resolved lineage for columns
        in the SELECT list and all relevant WHERE clauses.
        """
        if not self.final_select: return []

        results = []

        # --- Part 1: Analyze ALL columns from the final SELECT list ---
        for col_expr in self.final_select.expressions:
            # The incorrect "if not is_direct" condition has been removed.
            # We now process every single column from the SELECT list.
            entry = self._analyze_expression(col_expr, col_expr.alias_or_name, "SELECT")
            results.append(entry)

        # --- Part 2: Recursively find and analyze all WHERE clauses ---
        self._find_and_analyze_all_where_clauses(self.final_select, "Final Select", set(), results)

        return results

    def _analyze_expression(self, expression: Expression, name: str, source_clause: str) -> Dict[str, Any]:
        """Analyzes a column or expression from the SELECT list."""
        final_expression_ast = self._resolve_expression_fully(expression, self.final_select, set())
        is_direct = isinstance(final_expression_ast, exp.Column)
        final_expression_sql = final_expression_ast.sql(dialect=self.dialect)
        base_columns = {c.sql(dialect=self.dialect) for c in final_expression_ast.find_all(exp.Column)}

        return {
            "item": name,
            "source_clause": source_clause,
            "type": "base" if is_direct else "expression",
            "final_expression": None if is_direct else final_expression_sql,
            "base_columns": sorted(list(base_columns))
        }

    def _split_conditions_by_and(self, expression: Expression) -> List[Expression]:
        """Recursively splits a boolean expression by the AND operator."""
        if isinstance(expression, exp.And):
            return self._split_conditions_by_and(expression.left) + self._split_conditions_by_and(expression.right)
        else:
            return [expression]

    def _find_and_analyze_all_where_clauses(
            self, scope: exp.Select, context_name: str, visited: Set, results: List
    ):
        """Recursively finds and analyzes individual WHERE conditions."""
        if id(scope) in visited: return
        visited.add(id(scope))

        where_clause_node = scope.args.get('where')
        if where_clause_node:
            atomic_conditions = self._split_conditions_by_and(where_clause_node.this)
            for condition in atomic_conditions:
                resolved_condition_ast = self._resolve_expression_fully(condition, scope, set())
                base_columns_in_condition = {
                    c.sql(dialect=self.dialect)
                    for c in resolved_condition_ast.find_all(exp.Column)
                }
                results.append({
                    "item": f"Filter in {context_name}",
                    "source_clause": "WHERE",
                    "type": "filter_condition",
                    "filter_condition": resolved_condition_ast.sql(dialect=self.dialect),
                    "base_columns": sorted(list(base_columns_in_condition))
                })

        for source in scope.find_all(exp.From, exp.Join):
            source_item = source.this
            next_scope = None
            next_context_name = ""
            if isinstance(source_item, exp.Table):
                table_name = source_item.name.upper()
                if table_name in self.cte_definitions:
                    next_scope = self.cte_definitions[table_name]
                    next_context_name = f"CTE: {table_name}"
            elif isinstance(source_item, (exp.Subquery, exp.CTE)):
                next_scope = source_item.this
                next_context_name = f"Subquery: {source_item.alias_or_name}"
            if next_scope:
                self._find_and_analyze_all_where_clauses(next_scope, next_context_name, visited, results)

    def _resolve_expression_fully(self, expression: Expression, scope: exp.Select, visited: Set) -> Expression:
        def _resolver(node):
            if isinstance(node, exp.Column):
                return self._trace_and_replace_column(node, scope, visited.copy())
            return node

        resolved_ast = expression.transform(_resolver, copy=True)
        if isinstance(resolved_ast, exp.Alias):
            return resolved_ast.this
        return resolved_ast

    def _trace_and_replace_column(self, column: exp.Column, scope: exp.Select, visited: Set) -> Expression:
        column_name = column.name.upper()
        table_alias = column.table.upper() if column.table else None
        trace_id = (id(scope), table_alias, column_name)
        if trace_id in visited: return column
        visited.add(trace_id)
        source = self._find_source_for_alias(table_alias, scope)
        if not source: return column
        source_type, source_name, source_node = source
        if source_type == "table":
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
                    return self._resolve_expression_fully(sub_expr, source_node, visited)
        return column

    def _find_source_for_alias(
            self, alias: Optional[str], scope: exp.Select
    ) -> Optional[Tuple[str, str, Expression]]:
        sources = scope.find_all(exp.From, exp.Join)
        for source in sources:
            source_item = source.this
            source_alias = source_item.alias_or_name.upper()
            if alias and alias != source_alias: continue
            if isinstance(source_item, exp.Table):
                table_name = source_item.name.upper()
                if table_name in self.cte_definitions:
                    return "cte", table_name, self.cte_definitions[table_name]
                else:
                    return "table", table_name, source_item
            if isinstance(source_item, (exp.Subquery, exp.CTE)):
                return "subquery", source_alias, source_item.this
        return None


if __name__ == "__main__":
    full_query = """
    SELECT
    "D1"."C0" AS "Sold_To_Hierarchy_Level_5_Label", 
    "D1"."C1" AS "Salesman_ID", 
    "D1"."C2" AS "Salesman_Label", 
    "D1"."C3" AS "Salesman_Hierarchy_Level_1_ID", 
    "D1"."C4" AS "Salesman_Hierarchy_Level_1_Label", 
    SUM("D1"."C10") AS "Order_Qty__Not_Cancelled_", 
    SUM("D1"."C11") AS "Order_Qty__Prior_Working_Day_", 
    SUM("D1"."C12") AS "Billed_Units", 
    "D1"."C5" AS "PAK_Hierarchy_Level_2_Code", 
    "D1"."C6" AS "PAK_Hierarchy_Level_2_Desc", 
    "D1"."C7" AS "Month0", 
    "D1"."C8" AS "Month_Name", 
    "D1"."C9" AS "Year0"
FROM
    (
    SELECT
        "CUST_LCL_HIER_OTC__SOLDTO_"."CUST_HIER_LVL_5_LBL" AS "C0", 
        "VENDOR_DESC_ZZE1_CURR__SLSMN_"."VEND_ID" AS "C1", 
        "VENDOR_DESC_ZZE1_CURR__SLSMN_"."VEND_ID" || ' - ' || "VENDOR_DESC_ZZE1_CURR__SLSMN_"."VEND_NAME" AS "C2", 
        "VENDOR_DESC_ZZE1_CURR__SLSMN_LVL2_"."VEND_ID" AS "C3", 
        "VENDOR_DESC_ZZE1_CURR__SLSMN_LVL2_"."VEND_ID" || ' - ' || "VENDOR_DESC_ZZE1_CURR__SLSMN_LVL2_"."VEND_NAME" AS "C4", 
        CASE 
            WHEN 
                "PAK"."PAK_HIER_LVL_2_CD" IN ( 
                    '10110' )
                THEN
                    'Sommer'
            WHEN 
                "PAK"."PAK_HIER_LVL_2_CD" IN ( 
                    '10120' )
                THEN
                    'Winter'
            WHEN 
                "PAK"."PAK_HIER_LVL_2_CD" IN ( 
                    '10130' )
                THEN
                    'All Season'
            ELSE 'Nicht zugeordnet Saison'
        END AS "C5", 
        "PAK"."PAK_HIER_LVL_2_DESC" AS "C6", 
        EXTRACT(MONTH FROM "OTC_MART"."MONTH_DT") AS "C7", 
        CASE 
            WHEN EXTRACT(MONTH FROM "OTC_MART"."MONTH_DT") = '1' THEN 'January'
            WHEN EXTRACT(MONTH FROM "OTC_MART"."MONTH_DT") = '2' THEN 'February'
            WHEN EXTRACT(MONTH FROM "OTC_MART"."MONTH_DT") = '3' THEN 'March'
            WHEN EXTRACT(MONTH FROM "OTC_MART"."MONTH_DT") = '4' THEN 'April'
            WHEN EXTRACT(MONTH FROM "OTC_MART"."MONTH_DT") = '5' THEN 'May'
            WHEN EXTRACT(MONTH FROM "OTC_MART"."MONTH_DT") = '6' THEN 'June'
            WHEN EXTRACT(MONTH FROM "OTC_MART"."MONTH_DT") = '7' THEN 'July'
            WHEN EXTRACT(MONTH FROM "OTC_MART"."MONTH_DT") = '8' THEN 'August'
            WHEN EXTRACT(MONTH FROM "OTC_MART"."MONTH_DT") = '9' THEN 'September'
            WHEN EXTRACT(MONTH FROM "OTC_MART"."MONTH_DT") = '10' THEN 'October'
            WHEN EXTRACT(MONTH FROM "OTC_MART"."MONTH_DT") = '11' THEN 'November'
            WHEN EXTRACT(MONTH FROM "OTC_MART"."MONTH_DT") = '12' THEN 'December'
            ELSE '0'
        END AS "C8", 
        EXTRACT(YEAR FROM "OTC_MART"."MONTH_DT") AS "C9", 
        "OTC_MART"."ORD_QTY" AS "C10", 
        "OTC_MART"."LAST_WRK_DAY_ORD_QTY" AS "C11", 
        "OTC_MART"."SLS_QTY" AS "C12"
    FROM
        "PROD"."EU_BI_VWS"."DISTR_CHAN" "DISTR_CHAN"
            INNER JOIN "PROD"."EU_BI_VWS"."OTC_MART" "OTC_MART"
            ON "DISTR_CHAN"."DISTR_CHAN_CD" = "OTC_MART"."DISTR_CHAN_CD"
                LEFT OUTER JOIN "PROD"."EU_BI_VWS"."CNTRY_HIER_CO_SO_PC_OTC" "CNTRY_HIER_CO_SO_PC_OTC"
                ON 
                    "CNTRY_HIER_CO_SO_PC_OTC"."CO_CD" = "OTC_MART"."ORIG_CO_CD" AND
                    "CNTRY_HIER_CO_SO_PC_OTC"."SALES_ORG_CD" = "OTC_MART"."SALES_ORG_CD" AND
                    "CNTRY_HIER_CO_SO_PC_OTC"."PROFIT_CNTR_ID" = "OTC_MART"."PROFIT_CNTR_ID"
                    LEFT OUTER JOIN "PROD"."EU_BI_VWS"."CUST_LCL_HIER_OTC" "CUST_LCL_HIER_OTC__PAYER_"
                    ON "CUST_LCL_HIER_OTC__PAYER_"."CUST_ID_KEY" = "OTC_MART"."PAY_CUST_ID_KEY"
                        LEFT OUTER JOIN "PROD"."EU_BI_VWS"."CUST_LCL_HIER_OTC" "CUST_LCL_HIER_OTC__SOLDTO_"
                        ON "CUST_LCL_HIER_OTC__SOLDTO_"."CUST_ID_KEY" = "OTC_MART"."CUST_ID_KEY"
                            LEFT OUTER JOIN "PROD"."EU_BI_VWS"."VENDOR_DESC_ZZE1_CURR" "VENDOR_DESC_ZZE1_CURR__SLSMN_LVL2_"
                            ON "OTC_MART"."SLSMN_HIER_LVL_2_ID" = "VENDOR_DESC_ZZE1_CURR__SLSMN_LVL2_"."VEND_ID"
                                LEFT OUTER JOIN "PROD"."EU_BI_VWS"."VENDOR_DESC_ZZE1_CURR" "VENDOR_DESC_ZZE1_CURR__SLSMN_"
                                ON "OTC_MART"."SLSMN_ID" = "VENDOR_DESC_ZZE1_CURR__SLSMN_"."VEND_ID"
                                    LEFT OUTER JOIN "PROD"."EU_BI_VWS"."PAK" "PAK"
                                    ON "PAK"."PAK_ID" = "OTC_MART"."PAK_ID" 
    WHERE 
        "CNTRY_HIER_CO_SO_PC_OTC"."CO_CD" IN ( 
            'E211' ) AND
        "DISTR_CHAN"."DISTR_CHAN_CD" IN ( 
            '01' ) AND
        "PAK"."PAK_HIER_LVL_1_CD" IN ( 
            '10' ) AND
        EXTRACT(YEAR FROM "OTC_MART"."MONTH_DT") IN ( 
            '2025' ) AND
        EXTRACT(MONTH FROM "OTC_MART"."MONTH_DT") IN ( 
            '6' ) AND
        "CUST_LCL_HIER_OTC__PAYER_"."CUST_HIER_LVL_4_ID" = 'DE4RENZENT'
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
    "D1"."C9"
    """

    print("--- Query analysis including WHERE clause ---")
    analyzer = SQLLineageAnalyzer(full_query)
    lineage_data = analyzer.analyze()
    print(json.dumps(lineage_data, indent=2))