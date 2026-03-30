-- ============================================================
-- DAYLIGHT WEEKLY SALES SUMMARY (v2 — proper tax/shipping)
-- ============================================================
-- 1. Set 'target_monday' to the Monday that STARTS the target week.
-- 2. Comparison week is automatically the 7 days before.
-- 3. Taxes & shipping are summed per ORDER (not MAX across month).
-- ============================================================

SET target_monday = '2026-03-23'::DATE;

SET week1_start = $target_monday;
SET week1_end   = DATEADD('day', 6, $week1_start);
SET week2_start = DATEADD('day', -7, $week1_start);
SET week2_end   = DATEADD('day', -1, $week1_start);

-- ============================================================
-- ORDER-LEVEL AGGREGATION (correct tax/shipping)
-- ============================================================

WITH
-- Week 1: aggregate per order first, then sum across orders
w1_orders AS (
    SELECT
        NAME,
        SUM(CASE WHEN lineitem_sku IN ('1','6','6-k','100','200','300','301','400','401','7','303')
            THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_dc1,
        SUM(CASE WHEN lineitem_sku IN ('1','6','6-k','100','200','300','301','400','401','7','302','303','21','22','23','25','26','28','29','30','31','32','33','34','35','36','37','38','5000')
            THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_all,
        SUM(CASE WHEN lineitem_sku IN ('7','400','401')
            THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_kids,
        SUM(CASE WHEN lineitem_sku IN ('7','400','401')
            THEN lineitem_quantity ELSE 0 END) AS kids_units,
        SUM(CASE WHEN lineitem_sku IN ('1','6','100','200','300','301','303','400','401','7','302')
            THEN lineitem_quantity ELSE 0 END) AS gross_units,
        SUM(CASE WHEN lineitem_sku IN ('1','6','6-k','100','200','300','301','400','401','7','303') AND cancelled_at IS NULL
            THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_dc1_net,
        SUM(CASE WHEN cancelled_at IS NOT NULL THEN lineitem_quantity ELSE 0 END) AS cancelled_units,
        SUM(discount_amount) AS order_discounts,
        MAX(taxes) AS order_taxes,
        MAX(shipping) AS order_shipping
    FROM DAYLIGHT_SALES.CONNECTORS.SHOPIFY
    WHERE created_at::DATE BETWEEN $week1_start AND $week1_end
    GROUP BY NAME
),
w1 AS (
    SELECT
        SUM(line_dc1) + SUM(order_taxes) + SUM(order_shipping) AS gross_sales_dc1,
        SUM(line_all) + SUM(order_taxes) + SUM(order_shipping) AS gross_sales_all,
        SUM(line_dc1) AS line_revenue_dc1,
        SUM(line_kids) + SUM(CASE WHEN kids_units > 0 THEN order_taxes ELSE 0 END) + SUM(CASE WHEN kids_units > 0 THEN order_shipping ELSE 0 END) AS kids_rev,
        SUM(kids_units) AS kids_units,
        SUM(gross_units) AS gross_units,
        SUM(cancelled_units) AS cancelled_units,
        COUNT(*) AS order_count,
        SUM(order_discounts) AS discounts,
        SUM(line_dc1_net) - SUM(CASE WHEN cancelled_units = 0 THEN order_discounts ELSE 0 END) AS net_sales_dc1
    FROM w1_orders
),

-- Week 2: same structure
w2_orders AS (
    SELECT
        NAME,
        SUM(CASE WHEN lineitem_sku IN ('1','6','6-k','100','200','300','301','400','401','7','303')
            THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_dc1,
        SUM(CASE WHEN lineitem_sku IN ('1','6','6-k','100','200','300','301','400','401','7','302','303','21','22','23','25','26','28','29','30','31','32','33','34','35','36','37','38','5000')
            THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_all,
        SUM(CASE WHEN lineitem_sku IN ('7','400','401')
            THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_kids,
        SUM(CASE WHEN lineitem_sku IN ('7','400','401')
            THEN lineitem_quantity ELSE 0 END) AS kids_units,
        SUM(CASE WHEN lineitem_sku IN ('1','6','100','200','300','301','303','400','401','7','302')
            THEN lineitem_quantity ELSE 0 END) AS gross_units,
        SUM(CASE WHEN lineitem_sku IN ('1','6','6-k','100','200','300','301','400','401','7','303') AND cancelled_at IS NULL
            THEN lineitem_price * lineitem_quantity ELSE 0 END) AS line_dc1_net,
        SUM(CASE WHEN cancelled_at IS NOT NULL THEN lineitem_quantity ELSE 0 END) AS cancelled_units,
        SUM(discount_amount) AS order_discounts,
        MAX(taxes) AS order_taxes,
        MAX(shipping) AS order_shipping
    FROM DAYLIGHT_SALES.CONNECTORS.SHOPIFY
    WHERE created_at::DATE BETWEEN $week2_start AND $week2_end
    GROUP BY NAME
),
w2 AS (
    SELECT
        SUM(line_dc1) + SUM(order_taxes) + SUM(order_shipping) AS gross_sales_dc1,
        SUM(line_all) + SUM(order_taxes) + SUM(order_shipping) AS gross_sales_all,
        SUM(line_dc1) AS line_revenue_dc1,
        SUM(line_kids) + SUM(CASE WHEN kids_units > 0 THEN order_taxes ELSE 0 END) + SUM(CASE WHEN kids_units > 0 THEN order_shipping ELSE 0 END) AS kids_rev,
        SUM(kids_units) AS kids_units,
        SUM(gross_units) AS gross_units,
        SUM(cancelled_units) AS cancelled_units,
        COUNT(*) AS order_count,
        SUM(order_discounts) AS discounts,
        SUM(line_dc1_net) - SUM(CASE WHEN cancelled_units = 0 THEN order_discounts ELSE 0 END) AS net_sales_dc1
    FROM w2_orders
)

-- ============================================================
-- FINAL OUTPUT
-- ============================================================

SELECT 'Report: ' || $week1_start || ' vs ' || $week2_start AS metric,
       'Week 1 (Target)' AS week_1, 'Week 2 (Comp)' AS week_2, '% Change' AS pct_change

UNION ALL SELECT '=============== SALES ===============', '', '', ''

UNION ALL
SELECT 'Gross Sales DC-1',
    TO_VARCHAR(w1.gross_sales_dc1, '$999,999,999'),
    TO_VARCHAR(w2.gross_sales_dc1, '$999,999,999'),
    TO_VARCHAR(ROUND((w1.gross_sales_dc1 - w2.gross_sales_dc1) / NULLIF(w2.gross_sales_dc1, 0) * 100, 1)) || '%'
FROM w1, w2

UNION ALL
SELECT 'Gross Sales All Products',
    TO_VARCHAR(w1.gross_sales_all, '$999,999,999'),
    TO_VARCHAR(w2.gross_sales_all, '$999,999,999'),
    TO_VARCHAR(ROUND((w1.gross_sales_all - w2.gross_sales_all) / NULLIF(w2.gross_sales_all, 0) * 100, 1)) || '%'
FROM w1, w2

UNION ALL
SELECT 'Average Daily Sales (DC-1)',
    TO_VARCHAR(w1.gross_sales_dc1 / 7, '$999,999,999'),
    TO_VARCHAR(w2.gross_sales_dc1 / 7, '$999,999,999'),
    TO_VARCHAR(ROUND(((w1.gross_sales_dc1 / 7) - (w2.gross_sales_dc1 / 7)) / NULLIF(w2.gross_sales_dc1 / 7, 0) * 100, 1)) || '%'
FROM w1, w2

UNION ALL
SELECT 'Net Sales DC-1 (- canc, disc)',
    TO_VARCHAR(w1.net_sales_dc1, '$999,999,999'),
    TO_VARCHAR(w2.net_sales_dc1, '$999,999,999'),
    TO_VARCHAR(ROUND((w1.net_sales_dc1 - w2.net_sales_dc1) / NULLIF(w2.net_sales_dc1, 0) * 100, 1)) || '%'
FROM w1, w2

UNION ALL
SELECT 'Total Discounts',
    TO_VARCHAR(w1.discounts, '$999,999,999'),
    TO_VARCHAR(w2.discounts, '$999,999,999'),
    TO_VARCHAR(ROUND((w1.discounts - w2.discounts) / NULLIF(w2.discounts, 0) * 100, 1)) || '%'
FROM w1, w2

UNION ALL
SELECT 'Discount Rate %',
    TO_VARCHAR(ROUND(w1.discounts / NULLIF(w1.gross_sales_dc1, 0) * 100, 2)) || '%',
    TO_VARCHAR(ROUND(w2.discounts / NULLIF(w2.gross_sales_dc1, 0) * 100, 2)) || '%',
    TO_VARCHAR(ROUND((w1.discounts / NULLIF(w1.gross_sales_dc1, 0) * 100) - (w2.discounts / NULLIF(w2.gross_sales_dc1, 0) * 100), 2)) || ' pts'
FROM w1, w2

UNION ALL SELECT '=============== KIDS ===============', '', '', ''

UNION ALL
SELECT 'Kids Revenue',
    TO_VARCHAR(w1.kids_rev, '$999,999,999'),
    TO_VARCHAR(w2.kids_rev, '$999,999,999'),
    TO_VARCHAR(ROUND((w1.kids_rev - w2.kids_rev) / NULLIF(w2.kids_rev, 0) * 100, 1)) || '%'
FROM w1, w2

UNION ALL
SELECT 'Kids Units Sold',
    TO_VARCHAR(w1.kids_units),
    TO_VARCHAR(w2.kids_units),
    TO_VARCHAR(ROUND((w1.kids_units - w2.kids_units) / NULLIF(w2.kids_units, 0) * 100, 1)) || '%'
FROM w1, w2

UNION ALL
SELECT 'Kids % of Total Revenue',
    TO_VARCHAR(ROUND(w1.kids_rev / NULLIF(w1.gross_sales_dc1, 0) * 100, 1)) || '%',
    TO_VARCHAR(ROUND(w2.kids_rev / NULLIF(w2.gross_sales_dc1, 0) * 100, 1)) || '%',
    TO_VARCHAR(ROUND((w1.kids_rev / NULLIF(w1.gross_sales_dc1, 0) * 100) - (w2.kids_rev / NULLIF(w2.gross_sales_dc1, 0) * 100), 1)) || ' pts'
FROM w1, w2

UNION ALL
SELECT 'Kids % of Total Units',
    TO_VARCHAR(ROUND(w1.kids_units / NULLIF(w1.gross_units, 0) * 100, 1)) || '%',
    TO_VARCHAR(ROUND(w2.kids_units / NULLIF(w2.gross_units, 0) * 100, 1)) || '%',
    TO_VARCHAR(ROUND((w1.kids_units / NULLIF(w1.gross_units, 0) * 100) - (w2.kids_units / NULLIF(w2.gross_units, 0) * 100), 1)) || ' pts'
FROM w1, w2

UNION ALL SELECT '=============== UNITS ===============', '', '', ''

UNION ALL
SELECT 'Order Count',
    TO_VARCHAR(w1.order_count),
    TO_VARCHAR(w2.order_count),
    TO_VARCHAR(ROUND((w1.order_count - w2.order_count) / NULLIF(w2.order_count, 0) * 100, 1)) || '%'
FROM w1, w2

UNION ALL
SELECT 'Total Units Sold',
    TO_VARCHAR(w1.gross_units),
    TO_VARCHAR(w2.gross_units),
    TO_VARCHAR(ROUND((w1.gross_units - w2.gross_units) / NULLIF(w2.gross_units, 0) * 100, 1)) || '%'
FROM w1, w2

UNION ALL
SELECT 'Cancelled Units',
    TO_VARCHAR(w1.cancelled_units),
    TO_VARCHAR(w2.cancelled_units),
    TO_VARCHAR(ROUND((w1.cancelled_units - w2.cancelled_units) / NULLIF(w2.cancelled_units, 0) * 100, 1)) || '%'
FROM w1, w2

UNION ALL SELECT '=============== CALCULATED ===============', '', '', ''

UNION ALL
SELECT 'AOV (DC-1)',
    TO_VARCHAR(ROUND(w1.gross_sales_dc1 / NULLIF(w1.order_count, 0), 2), '$999,999'),
    TO_VARCHAR(ROUND(w2.gross_sales_dc1 / NULLIF(w2.order_count, 0), 2), '$999,999'),
    TO_VARCHAR(ROUND(((w1.gross_sales_dc1 / NULLIF(w1.order_count, 0)) - (w2.gross_sales_dc1 / NULLIF(w2.order_count, 0))) / NULLIF(w2.gross_sales_dc1 / NULLIF(w2.order_count, 0), 0) * 100, 1)) || '%'
FROM w1, w2

UNION ALL
SELECT 'Revenue per Unit',
    TO_VARCHAR(ROUND(w1.gross_sales_dc1 / NULLIF(w1.gross_units, 0), 2), '$999,999'),
    TO_VARCHAR(ROUND(w2.gross_sales_dc1 / NULLIF(w2.gross_units, 0), 2), '$999,999'),
    TO_VARCHAR(ROUND(((w1.gross_sales_dc1 / NULLIF(w1.gross_units, 0)) - (w2.gross_sales_dc1 / NULLIF(w2.gross_units, 0))) / NULLIF(w2.gross_sales_dc1 / NULLIF(w2.gross_units, 0), 0) * 100, 1)) || '%'
FROM w1, w2

UNION ALL
SELECT 'Cancellation Rate %',
    TO_VARCHAR(ROUND(w1.cancelled_units / NULLIF(w1.gross_units, 0) * 100, 2)) || '%',
    TO_VARCHAR(ROUND(w2.cancelled_units / NULLIF(w2.gross_units, 0) * 100, 2)) || '%',
    TO_VARCHAR(ROUND((w1.cancelled_units / NULLIF(w1.gross_units, 0) * 100) - (w2.cancelled_units / NULLIF(w2.gross_units, 0) * 100), 2)) || ' pts'
FROM w1, w2
;
