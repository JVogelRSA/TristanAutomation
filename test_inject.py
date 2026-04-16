import pandas as pd
from inventory_bot import SKU_MAP, TOP_PRIORITY_SKUS

curr_df = pd.DataFrame({'Item #': ['1', '32', '31'], 'Q On Hand': [256, 206, 432]})
summary_data = []
for index, row in curr_df.iterrows():
    sku = str(row['Item #']).strip()
    product_name = SKU_MAP.get(sku, sku) 
    summary_data.append({'SKU': sku, 'Product': product_name, 'Top 10': 'No', 'Current Stock': row['Q On Hand'], 'Avg Wkly Burn': 0, 'Runway (Est)': 'N/A'})

processed_skus = [d['SKU'] for d in summary_data]
for missing_sku, missing_product in SKU_MAP.items():
    if missing_sku not in processed_skus:
        is_top = "Yes" if missing_sku in TOP_PRIORITY_SKUS else "No"
        summary_data.append({
            'SKU': missing_sku,
            'Product': missing_product,
            'Top 10': is_top,
            'Current Stock': 0,
            'Avg Wkly Burn': 0.0,
            'Runway (Est)': 'N/A'
        })

summary_df = pd.DataFrame(summary_data)
tracked_skus = list(SKU_MAP.keys())
active_items = summary_df[(summary_df['Avg Wkly Burn'] > 0) | (summary_df['SKU'].isin(tracked_skus))]
active_items = active_items.sort_values(by='Avg Wkly Burn', ascending=False)
print(active_items)
