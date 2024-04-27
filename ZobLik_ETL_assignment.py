import sqlite3
import pandas as pd

conn = sqlite3.connect('./Data Engineer_ETL Assignment.db')
print("---Connected to DB---")
sales_df = pd.read_sql_query("SELECT * FROM Sales", conn)
customer_df = pd.read_sql_query("SELECT * FROM Customers", conn)
orders_df = pd.read_sql_query("SELECT * FROM Orders", conn)
items_df = pd.read_sql_query("SELECT * FROM Items", conn)

sales_customer_df = pd.merge(sales_df, customer_df, on='customer_id')
sales_customer_orders_df = pd.merge(sales_customer_df, orders_df, on='sales_id')
merged_df = pd.merge(sales_customer_orders_df, items_df, on='item_id')

age_filter_df = merged_df[(merged_df['age'] >= 18) & (merged_df['age'] <= 35)]

final_df = age_filter_df.groupby(['customer_id', 'item_name'])['quantity'].sum().reset_index()
final_df = final_df[final_df['quantity'] > 0]

result_df = pd.merge(final_df, customer_df[['customer_id', 'age']], on='customer_id')
result_df = result_df[['customer_id', 'age', 'item_name', 'quantity']]

result_df.columns = ['Customer', 'Age', 'Item', 'Quantity']
print(final_df)
