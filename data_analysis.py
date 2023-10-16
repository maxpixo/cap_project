##Data Analysis and Visualization
import mysql.connector
import pandas as pd
import os
from util.mysql_connect import connect_to_database
import matplotlib.pyplot as plt
import matplotlib
from app_config import db_config
from colorama import Fore, Style

##Task 1: Find and plot which transaction type has the highest transaction count.
##Task 2: Find and plot which state has a high number of customers.
##Task 3: Find and plot the sum of all transactions for the top 10 customers and identify the highest transaction amount.


############################-- mysql connect --############################
def mysql_connect():
    
    conn = mysql.connector.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"]
    )
    return conn


##Task 1: Find and plot which transaction type has the highest transaction count.
def find_highest_transaction_count():

    conn = connect_to_database()
    cursor = conn.cursor()

    query = "SELECT TRANSACTION_TYPE, COUNT(*) AS TransactionCount FROM CDW_SAPP_CREDIT_CARD GROUP BY TRANSACTION_TYPE"
    cursor.execute(query)

    results = cursor.fetchall()
    transaction_data = pd.DataFrame(results, columns=['TransactionType', 'TransactionCount'])

    conn.close()

    # Sort the data in ascending order by 'TransactionCount'
    transaction_data = transaction_data.sort_values(by='TransactionCount', ascending=False)

    # Find the transaction type with the highest transaction count
    highest_transaction_type = transaction_data['TransactionType'][transaction_data['TransactionCount'].idxmax()]

    # Create a list of colors based on the 'TransactionType'
    colors = ['orangered' if type == highest_transaction_type else 'royalblue' for type in transaction_data['TransactionType']]

    # Plot the data with colors
    transaction_data.plot(kind='bar', x='TransactionType', y='TransactionCount', color=colors, legend=False)
    
    plt.xlabel('Transaction Type')
    plt.ylabel('Transaction Count')
    plt.title(f'Transaction Count by Type (Highest: {highest_transaction_type})')
    plt.xticks(rotation=45)

    # Save the plot as a .png file in the figures folder
    plt.savefig(os.path.join("figures", "which_transaction_type_has_the_highest_transaction_count.png"), dpi=300, bbox_inches='tight')
    print(Fore.GREEN + "The Plot Has Been Saved In figures Folder")
    print(Style.RESET_ALL)

    
    plt.show()



# Task 2: Find and plot which state has a high number of customers.
def find_state_with_high_customers():

    conn = connect_to_database()
    cursor = conn.cursor()

    query = "SELECT CUST_STATE, COUNT(*) AS CustomerCount FROM CDW_SAPP_CUSTOMER GROUP BY CUST_STATE"
    cursor.execute(query)

    results = cursor.fetchall()
    customer_data = pd.DataFrame(results, columns=['State', 'CustomerCount'])

    conn.close()
    # Sort the data in ascending order by 'TransactionCount'
    customer_data = customer_data.sort_values(by='CustomerCount', ascending=False)

    customer_data.plot(kind='bar', x='State', y='CustomerCount', legend=False)
    plt.xlabel('State')
    plt.ylabel('Customer Count')
    plt.title('Customer Count by State')

    # Save the plot as a .png file in the figures folder
    plt.savefig(os.path.join("figures", "which_state_has_a_high_number_of_customers.png"), dpi=300, bbox_inches='tight')
    print(Fore.GREEN + "The Plot Has Been Saved In figures Folder")
    print(Style.RESET_ALL)

    plt.show()


# Task 3: Find and plot the sum of all transactions for the top 10 customers.
def find_top_10_customers():

    conn = connect_to_database()
    cursor = conn.cursor()

    query = "SELECT CUST_SSN, SUM(TRANSACTION_VALUE) AS TotalTransaction FROM CDW_SAPP_CREDIT_CARD GROUP BY CUST_SSN " \
            "ORDER BY TotalTransaction DESC LIMIT 10"
    cursor.execute(query)

    results = cursor.fetchall()
    top_10_data = pd.DataFrame(results, columns=['CustomerSSN', 'TotalTransaction'])

    conn.close()


    top_10_data.plot(kind='bar', x='CustomerSSN', y='TotalTransaction', legend=False)
    plt.xlabel('Customer SSN')
    plt.ylabel('Total Transaction Value')
    plt.title('Total Transaction Value for Top 10 Customers')
    plt.xticks(rotation=45)


    # Save the plot as a .png file in the graphic folder
    plt.savefig(os.path.join("figures", "top_10_customers.png"), dpi=300, bbox_inches='tight')
    print(Fore.GREEN + "The Plot Has Been Saved In figures Folder")
    print(Style.RESET_ALL)

    plt.show()


