# -*- coding: utf-8 -*-
"""
Created on Mon Jul 22 20:35:57 2024

@author: LingaReddyPavan
"""
##Data Analysis: PEI Case study

import pandas as pd

#Load Customer Data for Analysis
customers_df = pd.read_excel("Customer.xls")
print(customers_df.head())
print(customers_df.info())

# Checking for the duplicates in  Customer_id's
duplicate_customers = customers_df[customers_df['Customer_ID'].duplicated()]

# Check for missing values in Customers data
print(customers_df.isnull().sum())

# Filling missing values with a default value for Country column
customers_df.fillna({'Country': 'Unknown'}, inplace=True)

#checking the Customers Dataframe for customers with Age > 120
invalid_ages = customers_df[~customers_df['Age'].between(0, 120)]

# Check if negative values are present in the Age column
negative_ages = customers_df[customers_df['Age'] < 0]
print(negative_ages)

#####################################################################
#Load Orders Data for Analysis
orders_df = pd.read_csv('Order.csv')
print(orders_df.head())
print(orders_df.info())
print(orders_df.columns)


# Checking for the duplicates in  Order_id's
duplicate_Orders = orders_df[orders_df['Order_ID'].duplicated()]

# Checking for missing values in Orders data
print(orders_df.isnull().sum())

orders_df.fillna({ 'Amount': 0.0}, inplace=True)

# Check if there are any negative Amount 
negative_amounts = orders_df[orders_df['Amount'] < 0]


#########################################################################


# Load Shippings Data in json format and normalise it to Analyse the Data 
import json
with open('Shipping.json') as f:
    shippings_data = json.load(f)
shippings_df = pd.json_normalize(shippings_data)
print(shippings_df.head())
print(shippings_df.info())

# Checking for the duplicates in Shipping_ids
duplicate_Shippings = shippings_df[shippings_df['Shipping_ID'].duplicated()]

# Check for missing values in Shippings data
print(shippings_df.isnull().sum())

shippings_df.fillna({'Status': 'Unknown'}, inplace=True)


#for ease of working..convert the json file to CSV
with open('Shipping.json', 'r') as json_file:
   json_data = json.load(json_file)
df = pd.json_normalize(json_data)
df.to_csv('Shipping.csv', index=False)


###############################################################################

# checking if  all Customer_id's in Orders data exist in Customers
missing_customers = orders_df[~orders_df['Customer_ID'].isin(customers_df['Customer_ID'])]

# checking for all the Customer_ids in Shippings exist in Customers
missing_customers_shipping = shippings_df[~shippings_df['Customer_ID'].isin(customers_df['Customer_ID'])]




#####################################Questions #############################

#1.the total amount spent and the country for the Pending delivery status for each country

# Merge Orders and Shippings Data
merged_customers_orders_shipping = pd.merge(customers_df,orders_df,on='Customer_ID', how='inner')
Amount_spent_by_country=merged_customers_orders_shipping.groupby('Country')['Amount'].sum().reset_index()
print(Amount_spent_by_country)

#Merge Customers and Shipping
merged_customers_shipping=pd.merge(customers_df,shippings_df,on='Customer_ID', how='inner')
#Filtering where status is Pending
pending_deliveries = merged_customers_shipping[merged_customers_shipping['Status'] == 'Pending']
#Grouping by country 
Country_Pending_Delivery_count = pending_deliveries.groupby('Country')['Status'].count().reset_index()
#renaming Columns foir better Understanding
Country_Pending_Delivery_count=Country_Pending_Delivery_count.rename(columns={'Status':'Pending_Delivery_Count'})
print(Country_Pending_Delivery_count)

Final_Df=pd.merge(Amount_spent_by_country,Country_Pending_Delivery_count,on='Country',how='inner')

print(Final_Df)





#2.the total number of transactions, total quantity sold, and total amount spent for each customer, along with the product details.

# Group by Customer_ID and aggregate the required information
customer_summary = orders_df.groupby('Customer_ID').agg({'Order_ID': 'count','Amount': 'sum'}).reset_index()

customer_summary.columns

customer_summary = customer_summary.rename(columns={'Order_ID': 'total_transactions', 'Amount': 'total_amount_spent'})


# Merge with customer information
customer_summary = pd.merge(customer_summary, customers_df, on='Customer_ID', how='inner')

# Concatenate product details for each customer
product_details = orders_df.groupby('Customer_ID')['Item'].apply(', '.join).reset_index()
product_details.columns
product_details=product_details.rename(columns={'Item':'Items_list'})
product_details.head()


customer_summary = pd.merge(customer_summary, product_details, on='Customer_ID', how='inner')
customer_summary['Full Name']=customer_summary['First']+' '+customer_summary['Last']
customer_summary.columns
customer_summary = customer_summary.drop(columns=['First', 'Last','Country','Age'])
print(customer_summary.head())



#3.the maximum product purchased for each country.

# Merge Orders with Customers to get the Country information
orders_df.head()
customers_df.head()
orders_with_customers = pd.merge(orders_df, customers_df[['Customer_ID', 'Country']], on='Customer_ID', how='inner')
orders_with_customers.head()

# Group by Country and Item and calculate the total quantity sold for each product in each country
product_quantity = orders_with_customers.groupby(['Country', 'Item']).agg({'Amount': 'sum'}).reset_index()
print(product_quantity.columns) #checking column names
product_quantity=product_quantity.rename(columns={'Amount':'total_amount'})

# Identify the maximum product purchased for each country
idx = product_quantity.groupby('Country')['total_amount'].idxmax()
max_product_per_country = product_quantity.loc[idx].reset_index(drop=True)

print(max_product_per_country)



#4.the most purchased product based on the age category less than 30 and above 30.

# Create\ing age categories <30 and 30 or above
customers_df['AgeCategory'] = customers_df['Age'].apply(lambda x: '<30' if x < 30 else '>=30')

# Merging Orders with Customers including AgeCategory
orders_with_age_category = pd.merge(orders_df, customers_df[['Customer_ID', 'AgeCategory']], on='Customer_ID', how='inner')

# Group by AgeCategory and Item and calculate the total quantity sold for each product in each age category

product_quantity_age_cat= orders_with_age_category.groupby(['AgeCategory', 'Item']).agg({'Order_ID':'count'}).reset_index()
product_quantity_age_cat.columns
#rENAMING Columns
product_quantity_age_cat=product_quantity_age_cat.rename(columns={'Order_ID':'Order_count'})

sorted_product_quantity = product_quantity_age_cat.sort_values(by=['AgeCategory', 'Order_count'], ascending=[True, False])
max_product_per_age_category = sorted_product_quantity.drop_duplicates(subset=['AgeCategory'], keep='first').reset_index(drop=True)




#5.the country that had minimum transactions and sales amount


# Group by Country and calculate the total number of transactions and total amount spent
country_summary = orders_with_customers.groupby('Country').agg(total_transactions=('Order_ID', 'count'), total_amount_spent=('Amount', 'sum')
).reset_index()

country_summary.shape


min_transactions_country = country_summary.sort_values(by='total_transactions').iloc[0]

min_sales_country = country_summary.sort_values(by='total_amount_spent').iloc[0]


"""
The Results generated using this Analysis are used to compare and cross validate the same via Visualizations in Tableau

"""






