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

    query2 = """
    SELECT  
    ORIG_CO_CD || '$' || SALES_ORG_CD || '$' || PROFIT_CNTR_ID AS "ORIG_CO_CD_SALES_ORG_CD_PROFIT_CNTR_ID_JOIN_KEY",
    ORIG_SYS_ID || '$' || OTC_MART.MONTH_DT || '$' || OTC_MART.MATL_ID AS "ORIG_SYS_ID_MONTH_DT_MATL_ID_JOIN_KEY",
    ORIG_SYS_ID || '$' || PAY_CUST_ID_KEY  AS "ORIG_SYS_ID_PAY_CUST_ID_KEY_JOIN_KEY",
    ORIG_SYS_ID || '$' || PAY_CUST_ID  AS "ORIG_SYS_ID_PAY_CUST_ID_JOIN_KEY",
    ORIG_SYS_ID || '$' || COB_DISTR_CHAN_CD  AS "ORIG_SYS_ID_COB_DISTR_CHAN_CD_JOIN_KEY",
    ORIG_SYS_ID || '$' || SALES_ORG_CD|| '$' || DISTR_CHAN_CD || '$' || PAY_CUST_ID AS "ORIG_SYS_ID_SALES_ORG_CD_DISTR_CHAN_CD_PAY_CUST_ID_JOIN_KEY",
    OTC_MART.MATL_ID || '$' || CNTRY_NAME_CD  AS "MATL_ID_CNTRY_NAME_CD_JOIN_KEY",
    ORIG_SYS_ID || '$' || SALES_ORG_CD || '$' || OTC_MART.ORIG_CO_CD AS "ORIG_SYS_ID_SALES_ORG_CD_ORIG_CO_CD_JOIN_KEY",
    OTC_MART.ORIG_SYS_ID || '$' || SHIP_TO_CUST_ID  AS "ORIG_SYS_ID_SHIP_TO_CUST_ID_JOIN_KEY",
    ORIG_SYS_ID || '$' || SALES_ORG_CD|| '$' || DISTR_CHAN_CD || '$' || SHIP_TO_CUST_ID AS "ORIG_SYS_ID_SALES_ORG_CD_DISTR_CHAN_CD_SHIP_TO_CUST_ID_JOIN_KEY",
    OTC_MART.ORIG_SYS_ID || '$' || SOLD_TO_CUST_ID  AS "ORIG_SYS_ID_SOLD_TO_CUST_ID_JOIN_KEY",
    ORIG_SYS_ID || '$' || SALES_ORG_CD|| '$' || DISTR_CHAN_CD || '$' || SOLD_TO_CUST_ID AS "ORIG_SYS_ID_SALES_ORG_CD_DISTR_CHAN_CD_SOLD_TO_CUST_ID_JOIN_KEY",
    OTC_MART.ORIG_SYS_ID || '$' || SOLD_TO_CUST_ID || '$' || OTC_MART.MATL_ID AS "ORIG_SYS_ID_SOLD_TO_CUST_ID_MATL_ID_JOIN_KEY",
    OTC_MART.ORIG_SYS_ID || '$' || SALES_ORG_CD|| '$' || DISTR_CHAN_CD || '$' || OTC_MART.MATL_ID AS "ORIG_SYS_ID_SALES_ORG_CD_DISTR_CHAN_CD_MATL_ID_JOIN_KEY",
    IFNULL(OTC_MART.SLSMN_ID,'') || '$' || IFNULL(SLSMN_HIER_LVL_1_ID,'') || '$' || IFNULL(SLSMN_HIER_LVL_2_ID,'') || '$' || IFNULL(SLSMN_HIER_LVL_3_ID,'') AS "SLSMN_ID_SLSMN_HIER_LVL_1_ID_SLSMN_HIER_LVL_2_ID_SLSMN_HIER_LVL_3_ID_JOIN_KEY",
    IFNULL(OTC_MART.PAY_SLSMN_ID,'') || '$' || IFNULL(PAY_SLSMN_HIER_LVL_1_ID,'') || '$' || IFNULL(PAY_SLSMN_HIER_LVL_2_ID,'') || '$' || IFNULL(PAY_SLSMN_HIER_LVL_3_ID,'') AS "PAY_SLSMN_ID_PAY_SLSMN_HIER_LVL_1_ID_PAY_SLSMN_HIER_LVL_2_ID_PAY_SLSMN_HIER_LVL_3_ID_JOIN_KEY",
    IFNULL(OTC_MART.MONTH_DT,'1900-01-01')  || '$' || IFNULL(OTC_MART.CNTRY_NAME_CD,'1900-01-01')  AS "MONTH_DT_CNTRY_NAME_CD_JOIN_KEY",
    
    OTC_MART.BUS_INQR_QTY AS "Business_Inquiry_Qty", 
    OTC_MART.ORD_QTY AS "Order_Qty(Not Cancelled)", 
    OTC_MART.DELIV_QTY AS "Shipped_Units", 
    OTC_MART.FORC_DELIV_QTY AS "Estimated_Shipped_Units", 
    OTC_MART.SLS_QTY AS "Billed_Units", 
    OTC_MART.EXCL_FOC_ITM_SLS_QTY AS "Billed_Qty_excl_ZZFC", 
    OTC_MART.LAST_WRK_DAY_ORD_QTY AS "Order_Qty(Prior Working Day)", 
    OTC_MART.LAST_WRK_DAY_DELIV_QTY AS "Shipped_Units(Prior Working Day)", 
    OTC_MART.LAST_WRK_DAY_SLS_QTY AS "Billed_Units(Prior Working Day)", 
    OTC_MART.AOP_SALES_QTY AS "AOP_Qty", 
    OTC_MART.PAY_TGT_UNIT_QTY AS "Payer - AOP_Qty",
    OTC_MART.LST_EST_UNIT_QTY_1 AS "Latest_Estimate_1 - AOP_Qty", 
    OTC_MART.LST_EST_UNIT_QTY_2 AS "Latest_Estimate_2 - AOP_Qty", 
    OTC_MART.CONSENSUS_FORC_CLOS_LAG_1_QTY AS "Lag_1_PBU_MOR", 
    OTC_MART.CONSENSUS_FORC_CLOS_LAG_2_QTY AS "Lag_2_PBU_MOR", 
    OTC_MART.CONSENSUS_FORC_CLOS_LAG_3_QTY AS "Lag_3_PBU_MOR", 
    OTC_MART.CONSENSUS_FORC_CLOS_LAG_6_QTY AS "Lag_6_PBU_MOR", 
    OTC_MART.CNTRY_SLS_FORC_CLOS_LAG_2_QTY AS "Lag_2_Closing_Country_Sales_View", 
    OTC_MART.CNTRY_PRLMNRY_FORC_LAG_2_QTY AS "Lag_2_Country_Preliminary_FC", 
    OTC_MART.ADJ_DMAN_PLN_FORC_LAG_1_QTY AS "Lag_1_Adjusted_DP", 
    OTC_MART.ADJ_DMAN_PLN_FORC_LAG_2_QTY AS "Lag_2_Adjusted_DP", 
    OTC_MART.ADJ_DMAN_PLN_FORC_LAG_3_QTY AS "Lag_3_Adjusted_DP",
    OTC_MART.PBU_CNSS_FORC_LAG_2_QTY AS "Lag_2_PBU_Consensus_FC", 
    OTC_MART.MATL_AVAIL_DT_ORD_QTY AS "Order_Qty(Material Avail. Date)", 
    OTC_MART.MATL_AVAIL_DT_CNFRM_QTY AS "Confirmed_Qty(Material Avail. Date)", 
    OTC_MART.MATL_AVAIL_DT_OPEN_ORD_QTY AS "Open_Order_Qty(Material Avail. Date)", 
    OTC_MART.MATL_AVAIL_DT_OPEN_ORD_UNCNFRM_QTY AS "Open_Unconfirmed_Qty(Material Avail. Date)", 
    OTC_MART.MATL_AVAIL_DT_OPEN_DELIV_QTY AS "Open_Delivery_Qty(Material Avail. Date)", 
    OTC_MART.PDD_RMAIN_OPEN_ORD_QTY AS "Open_Order_Qty(Promised Delivery Date)", 
    OTC_MART.WAIT_LIST_1_BUS_INQR_QTY AS "Business_Inquiry_WLI_1_fRDD_Qty", 
    OTC_MART.WAIT_LIST_2_BUS_INQR_QTY AS "Business_Inquiry_WLI_2_fRDD_Qty", 
    OTC_MART.WAIT_LIST_3_BUS_INQR_QTY AS "Business_Inquiry_WLI_3_fRDD_Qty", 
    OTC_MART.WAIT_LIST_4_BUS_INQR_QTY AS "Business_Inquiry_WLI_4_fRDD_Qty", 
    OTC_MART.WAIT_LIST_WTHT_CD_BUS_INQR_QTY AS "Business_Inquiry_no_WLI_RDD_Qty",
    OTC_MART.REJ_GDYR_ORD_BUS_INQR_QTY AS "Rejected_GDT", 
    OTC_MART.REJ_CUST_ORD_BUS_INQR_QTY AS "Rejected_Customer", 
    OTC_MART.PGI_ON_DELIV_QTY AS "Open_PGI_by_Planned_PGI_Date", 
    OTC_MART.PGI_OPEN_ORD_QTY AS "Open_Orders_by_Planned_PGI_Date", 
    OTC_MART.THRD_PTY_ORD_QTY AS "ZGTA_ZGRE_Order_Qty", 
    OTC_MART.DMAN_QTY AS "Demand_Qty", 
    OTC_MART.FULFILL_GAP_QTY AS "Fulfilment_Gap_Qty", 
    OTC_MART.PGI_OPEN_ORD_RTRN_QTY AS "PGI_Open_Order_Return_Qty", 
    OTC_MART.DELIV_RTRN_QTY AS "Delivery_Return_Qty",
    OTC_MART.NET_SLS_AMT AS "Net_Sales", 
    0 AS "Ecotaxes_Net_Sales", 
    0 AS "Management_Cost", 
    0 AS "Imported_Product_Cost", 
    0 AS "Cost_To_Serve_Freight", 
    0 AS "Cost_To_Serve_VAS", 
    OTC_MART.COST_OF_SLS_AMT AS "Cost_of_Sales", 
    OTC_MART.GROSS_PRFT_AMT AS "Gross_Profit", 
    OTC_MART.PAY_TGT_NET_SLS_AMT AS "Payer_AOP_Net_Sales", 
    OTC_MART.PAY_TGT_GROSS_PRFT_AMT AS "Payer - AOP_Gross_Profit", 
    OTC_MART.LST_EST_NET_SLS_AMT_1 AS "Latest_Estimate_1 - AOP_Net_Sales", 
    OTC_MART.LST_EST_GROSS_PRFT_AMT_1 AS "Latest_Estimate_1 - AOP_Gross_Profit",
    OTC_MART.LST_EST_NET_SLS_AMT_2 AS "Latest_Estimate_2 - AOP_Net_Sales", 
    OTC_MART.LST_EST_GROSS_PRFT_AMT_2 AS "Latest_Estimate_2 - AOP_Gross_Profit", 
    CAST(OTC_MART.LIST_PRC_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "List_Price(Local)", 
    CAST(OTC_MART.DISC_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Discounts(Local)", 
    CAST(OTC_MART.CASING_DISC_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Casing_Discount(Local)", 
    CAST(OTC_MART.REBATE_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Rebates(Local)", 
    CAST(OTC_MART.LEASE_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Leasing(Local)", 
    CAST(OTC_MART.INVC_LVL_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Invoice_Level(Local)", 
    CAST(OTC_MART.INVC_LVL_INCL_FOC_DISC_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Invoice_Level_Including_FOC(Local)", 
    CAST(OTC_MART.INVC_LVL_GROSS_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Invoice_Level_Gross(Local)", 
    CAST(OTC_MART.REBATE_CALC_GROSS_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Rebate_Base - Gross(Local)",
    CAST(OTC_MART.GROSS_EX_NET_DEAL_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Rebate_Base - Gross_Excluding_Net_Deals(Local)", 
    CAST(OTC_MART.NET_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Rebate_Base - Net_Excluding_Surcharges(Local)", 
    CAST(OTC_MART.NET_EX_NET_DEAL_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Rebate_Base - Net_Excluding_Net_Deals(Local)", 
    CAST(OTC_MART.NET_SLS_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Net_Sales(Local)", 
    0 AS "Ecotaxes_Net_Sales(Local)", 
    0 AS "Management_Cost(Local)", 
    0 AS "Imported_Product_Cost(Local)", 
    0 AS "Cost_To_Serve_Freight(Local)", 
    0 AS "Cost_To_Serve_VAS(Local)", 
    CAST(OTC_MART.COST_OF_SLS_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Cost_of_Sales(Local)", 
    CAST(OTC_MART.GROSS_PRFT_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Gross_Profit(Local)",
    CAST(OTC_MART.PAY_TGT_NET_SLS_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Payer - AOP_Net_Sales(Local)", 
    CAST(OTC_MART.PAY_TGT_GROSS_PRFT_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Payer - AOP_Gross_Profit(Local)", 
    CAST(OTC_MART.LST_EST_NET_SLS_AMT_1 AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Latest_Estimate_1 - AOP_Net_Sales(Local)", 
    CAST(OTC_MART.LST_EST_GROSS_PRFT_AMT_1 AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Latest_Estimate_1 - AOP_Gross_Profit(Local)", 
    CAST(OTC_MART.LST_EST_NET_SLS_AMT_2 AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Latest_Estimate_2 - AOP_Net_Sales(Local)", 
    CAST(OTC_MART.LST_EST_GROSS_PRFT_AMT_2 AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Latest_Estimate_2 - AOP_Gross_Profit(Local)", 
    OTC_MART.GRP_CRNCY_ID AS "Group Currency ID", 
    OTC_MART.LCL_CRNCY_ID AS "Local Currency ID", 
    OTC_MART.GRP_TO_LCL_EXCHG_RT AS "Group_to_Local_Exchange_Rate",
    OTC_MART.MONTH_DT AS "Month Date", 
    EXTRACT(YEAR FROM OTC_MART.MONTH_DT) AS "Year", 
    CASE EXTRACT(MONTH FROM OTC_MART.MONTH_DT)
        WHEN 1 THEN 'Q1'
        WHEN 2 THEN 'Q1'
        WHEN 3 THEN 'Q1'
        WHEN 4 THEN 'Q2'
        WHEN 5 THEN 'Q2'
        WHEN 6 THEN 'Q2'
        WHEN 7 THEN 'Q3'
        WHEN 8 THEN 'Q3'
        WHEN 9 THEN 'Q3'
        WHEN 10 THEN 'Q4'
        WHEN 11 THEN 'Q4'
        WHEN 12 THEN 'Q4'
        ELSE 'Q?'
    END AS "Quarter", 
    EXTRACT(YEAR FROM OTC_MART.MONTH_DT) || ' / ' || CASE EXTRACT(MONTH FROM OTC_MART.MONTH_DT)
        WHEN 1 THEN 'Q1'
        WHEN 2 THEN 'Q1'
        WHEN 3 THEN 'Q1'
        WHEN 4 THEN 'Q2'
        WHEN 5 THEN 'Q2'
        WHEN 6 THEN 'Q2'
        WHEN 7 THEN 'Q3'
        WHEN 8 THEN 'Q3'
        WHEN 9 THEN 'Q3'
        WHEN 10 THEN 'Q4'
        WHEN 11 THEN 'Q4'
        WHEN 12 THEN 'Q4'
        ELSE 'Q?'
    END AS "Quarter (YYYY/QQ)", 
    EXTRACT(MONTH FROM OTC_MART.MONTH_DT) AS "Month", 
    EXTRACT(YEAR FROM OTC_MART.MONTH_DT) * 100 + EXTRACT(MONTH FROM OTC_MART.MONTH_DT) AS "Month(YYYYMM)", 
    OTC_MART.REC_TYP_CD AS "Record Type", 
    OTC_MART.DISTR_CHAN_CD AS "DISTR CHAN CD" , 
    OTC_MART.PROFIT_CNTR_ID AS "Profit Center ID", 
    OTC_MART.CNTRY_NAME_CD AS "Country Code",  
    OTC_MART.CO_CD AS "Company Code", 
    OTC_MART.SALES_ORG_CD AS "Sales Org Code",
     OTC_MART.DESIGN AS "Design", 
    OTC_MART.MATL_ID AS "Material ID",
	OTC_MART.MATL_HIER_LVL_1_CD AS "Material Hierarchy Level 1 Code",
        MATL_HIER_DESC_EN_EAGLE_CURR_1.HIER_LVL_DESC AS "Material Hierarchy Level 1 Desc", 
        OTC_MART.MATL_HIER_LVL_1_CD || ' - ' || MATL_HIER_DESC_EN_EAGLE_CURR_1.HIER_LVL_DESC AS "Material Hierarchy Level 1 Label", 
        OTC_MART.MATL_HIER_LVL_2_CD AS "Material Hierarchy Level 2 Code", 
        MATL_HIER_DESC_EN_EAGLE_CURR_2.HIER_LVL_DESC AS "Material Hierarchy Level 2 Desc", 
        OTC_MART.MATL_HIER_LVL_2_CD || ' - ' || MATL_HIER_DESC_EN_EAGLE_CURR_2.HIER_LVL_DESC AS "Material Hierarchy Level 2 Label", 
        OTC_MART.MATL_HIER_LVL_3_CD AS "Material Hierarchy Level 3 Code", 
        MATL_HIER_DESC_EN_EAGLE_CURR_3.HIER_LVL_DESC AS "Material Hierarchy Level 3 Desc", 
        OTC_MART.MATL_HIER_LVL_3_CD || ' - ' || MATL_HIER_DESC_EN_EAGLE_CURR_3.HIER_LVL_DESC AS "Material Hierarchy Level 3 Label", 
        OTC_MART.MATL_HIER_LVL_4_CD AS "Material Hierarchy Level 4 Code", 
        MATL_HIER_DESC_EN_EAGLE_CURR_4.HIER_LVL_DESC AS "Material Hierarchy Level 4 Desc", 
        OTC_MART.MATL_HIER_LVL_4_CD || ' - ' || MATL_HIER_DESC_EN_EAGLE_CURR_4.HIER_LVL_DESC AS "Material Hierarchy Level 4 Label",
	OTC_MART.MATL_HIER_LVL_5_CD AS "Material Hierarchy Level 5 Code",
        MATL_HIER_DESC_EN_EAGLE_CURR_5.HIER_LVL_DESC AS "Material Hierarchy Level 5 Desc", 
        OTC_MART.MATL_HIER_LVL_5_CD || ' - ' || MATL_HIER_DESC_EN_EAGLE_CURR_5.HIER_LVL_DESC AS "Material Hierarchy Level 5 Label", 
        OTC_MART.MATL_HIER_LVL_6_CD AS "Material Hierarchy Level 6 Code", 
        MATL_HIER_DESC_EN_EAGLE_CURR_6.HIER_LVL_DESC AS "Material Hierarchy Level 6 Desc", 
        OTC_MART.MATL_HIER_LVL_6_CD || ' - ' || MATL_HIER_DESC_EN_EAGLE_CURR_6.HIER_LVL_DESC AS "Material Hierarchy Level 6 Label", 
        OTC_MART.MATL_HIER_LVL_7_CD AS "Material Hierarchy Level 7 Code", 
        MATL_HIER_DESC_EN_EAGLE_CURR_7.HIER_LVL_DESC AS "Material Hierarchy Level 7 Desc", 
        OTC_MART.MATL_HIER_LVL_7_CD || ' - ' || MATL_HIER_DESC_EN_EAGLE_CURR_7.HIER_LVL_DESC AS "Material Hierarchy Level 7 Label", 
        OTC_MART.MATL_HIER_LVL_8_CD AS "Material Hierarchy Level 8 Code", 
        MATL_HIER_DESC_EN_EAGLE_CURR_8.HIER_LVL_DESC AS "Material Hierarchy Level 8 Desc", 
        OTC_MART.MATL_HIER_LVL_8_CD || ' - ' || MATL_HIER_DESC_EN_EAGLE_CURR_8.HIER_LVL_DESC AS "Material Hierarchy Level 8 Label",
	 OTC_MART.SOLD_TO_CUST_ID AS "Sold To Customer ID", 
    OTC_MART.CUST_ID_KEY AS "Sold To Customer ID Key", 
    OTC_MART.SHIP_TO_CUST_ID AS "Ship To Customer ID", 
    OTC_MART.SLSMN_ID AS "Selling Responsible ID", 
    OTC_MART.SLSMN_HIER_LVL_1_ID AS "Manager of Selling Responsible ID", 
    OTC_MART.SLSMN_HIER_LVL_2_ID AS "KAM Local Country ID", 
    OTC_MART.SLSMN_HIER_LVL_3_ID AS "KAM Pan EU ID", 
    OTC_MART.PAY_CUST_ID AS "Payer Customer ID", 
    OTC_MART.PAY_CUST_ID_KEY AS "Payer Customer ID Key", 
    OTC_MART.PAY_SLSMN_ID AS "Payer Selling Responsible ID", 
    OTC_MART.PAY_SLSMN_HIER_LVL_1_ID AS "Payer Manager of Selling Responsible ID",
    OTC_MART.PAY_SLSMN_HIER_LVL_2_ID AS "Payer KAM Local Country ID", 
    OTC_MART.PAY_SLSMN_HIER_LVL_3_ID AS "Payer KAM Pan EU ID", 
    OTC_MART.BRAND_ID AS "Brand ID", 
    OTC_MART.PAK_ID AS "PAK ID", 
    OTC_MART.CBU_CD AS "CBU Code", 
    OTC_MART.SPU_ID AS "SPU ID", 
    OTC_MART.ORIG_SYS_ID AS "Originating System ID",
     EURONET_BY_MONTH.COND_RT_AMT * OTC_MART.SLS_QTY AS "Net_Euronet", 
    CAST(OTC_MART.NET_SLS_AMT AS DOUBLE PRECISION) / NULLIF(EURONET_BY_MONTH.COND_RT_AMT * OTC_MART.SLS_QTY, 0) - 1 AS "Weighted_Euronet_Deviation", 
    CAST(OTC_MART.NET_SLS_AMT AS DOUBLE PRECISION) / NULLIF(OTC_MART.SLS_QTY, 0) AS "ANSP_1", 
    CAST(OTC_MART.GROSS_PRFT_AMT AS DOUBLE PRECISION) / NULLIF(OTC_MART.SLS_QTY, 0) AS "Gross_Profit_per_Tire", 
    CAST(OTC_MART.NET_SLS_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT / NULLIF(OTC_MART.SLS_QTY, 0) AS "ANSP(Local)", 
    CAST(OTC_MART.GROSS_PRFT_AMT AS FLOAT) * OTC_MART.GRP_TO_LCL_EXCHG_RT / NULLIF(OTC_MART.SLS_QTY, 0) AS  "Gross_Profit_per_Tire(Local)", 
    DATE_TRUNC('MONTH', CURRENT_DATE) AS "Current Month Start Date", 
    LAST_DAY(CURRENT_DATE) AS "Current Month End Date", 
    DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE)) AS "Prior Month Start Date", 
    LAST_DAY(DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE))) AS "Prior Month End Date"
   
    
FROM
    EU_BI_VWS.OTC_MART OTC_MART
	LEFT OUTER JOIN EU_BI_VWS.EURONET_MTH_SAP EURONET_BY_MONTH
        ON 
            OTC_MART.MATL_ID = EURONET_BY_MONTH.MATL_ID AND
            OTC_MART.MONTH_DT =EURONET_BY_MONTH.MONTH_DT
  
   	LEFT OUTER JOIN EU_BI_VWS.MATL_HIER_DESC_EN_EAGLE_CURR_1 MATL_HIER_DESC_EN_EAGLE_CURR_1
            ON MATL_HIER_DESC_EN_EAGLE_CURR_1.HIER_LVL_CD = OTC_MART.MATL_HIER_LVL_1_CD
                LEFT OUTER JOIN EU_BI_VWS.MATL_HIER_DESC_EN_EAGLE_CURR_2 MATL_HIER_DESC_EN_EAGLE_CURR_2
                ON MATL_HIER_DESC_EN_EAGLE_CURR_2.HIER_LVL_CD = OTC_MART.MATL_HIER_LVL_2_CD
                    LEFT OUTER JOIN EU_BI_VWS.MATL_HIER_DESC_EN_EAGLE_CURR_3 MATL_HIER_DESC_EN_EAGLE_CURR_3
                    ON MATL_HIER_DESC_EN_EAGLE_CURR_3.HIER_LVL_CD = OTC_MART.MATL_HIER_LVL_3_CD
                        LEFT OUTER JOIN EU_BI_VWS.MATL_HIER_DESC_EN_EAGLE_CURR_4 MATL_HIER_DESC_EN_EAGLE_CURR_4
                        ON MATL_HIER_DESC_EN_EAGLE_CURR_4.HIER_LVL_CD = OTC_MART.MATL_HIER_LVL_4_CD
	LEFT OUTER JOIN EU_BI_VWS.MATL_HIER_DESC_EN_EAGLE_CURR_5 MATL_HIER_DESC_EN_EAGLE_CURR_5
            ON MATL_HIER_DESC_EN_EAGLE_CURR_5.HIER_LVL_CD = OTC_MART.MATL_HIER_LVL_5_CD
                LEFT OUTER JOIN EU_BI_VWS.MATL_HIER_DESC_EN_EAGLE_CURR_6 MATL_HIER_DESC_EN_EAGLE_CURR_6
                ON MATL_HIER_DESC_EN_EAGLE_CURR_6.HIER_LVL_CD = OTC_MART.MATL_HIER_LVL_6_CD
                    LEFT OUTER JOIN EU_BI_VWS.MATL_HIER_DESC_EN_EAGLE_CURR_7 MATL_HIER_DESC_EN_EAGLE_CURR_7
                    ON MATL_HIER_DESC_EN_EAGLE_CURR_7.HIER_LVL_CD = OTC_MART.MATL_HIER_LVL_7_CD
                        LEFT OUTER JOIN EU_BI_VWS.MATL_HIER_DESC_EN_EAGLE_CURR_8 MATL_HIER_DESC_EN_EAGLE_CURR_8
                        ON MATL_HIER_DESC_EN_EAGLE_CURR_8.HIER_LVL_CD = OTC_MART.MATL_HIER_LVL_8_CD
--WHERE YEAR(OTC_MART.MONTH_DT )>= YEAR(CURRENT_DATE())- 1

    """
    print("--- Query analysis including WHERE clause ---")
    analyzer = SQLLineageAnalyzer(query2)
    lineage_data = analyzer.analyze()
    print(json.dumps(lineage_data, indent=2))