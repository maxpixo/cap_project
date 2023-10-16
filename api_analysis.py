import pandas as pd
import matplotlib.pyplot as plt
import mysql
import mysql.connector
import os
from util.mysql_connect import connect_to_database
from colorama import Fore, Style








############################-- percentage approved for self employed --############################
def percentage_approved_for_self_employedV2():

    
    
    try:
        # Connect to the MySQL database
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Query the data from the MySQL database
        query = f"SELECT Self_Employed, Application_Status FROM CDW_SAPP_loan_application"
        cursor.execute(query)

        # Fetch the results
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        # Create a DataFrame from the query results
        df = pd.DataFrame(results, columns=column_names)

        # Calculate the percentage of approvals for self-employed applicants
        self_employed_approval_percentage = (df[df['Self_Employed'] == 'Yes']['Application_Status'] == 'Y').mean() * 100

        # Create a pie chart to visualize the percentage
        labels = ['Approved', 'Not Approved']
        sizes = [self_employed_approval_percentage, 100 - self_employed_approval_percentage]

        # Plot the data
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
        plt.title('Percentage of Applications Approved for Self-Employed Applicants')
        plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

        # Save the plot as a .png file in the figures folder
        plt.savefig(os.path.join("figures", "percentage_approved_for_self_employed.png"), dpi=300, bbox_inches='tight')
        print(Fore.GREEN + "The Plot Has Been Saved In figures Folder")
        print(Style.RESET_ALL)

        plt.show()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        conn.close()



  


############################-- percentage rejection for married males --############################
def percentage_rejection_for_married_malesV2():
    

    try:
        # Connect to the MySQL database
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Query the data from the MySQL database
        query = f"SELECT Married, Gender, Application_Status FROM CDW_SAPP_loan_application"
        cursor.execute(query)

        # Fetch the results
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        # Create a DataFrame from the query results
        df = pd.DataFrame(results, columns=column_names)

        # Calculate the percentage of rejection for married males
        married_males_rejection_percentage = (df[(df['Married'] == 'Yes') & (df['Gender'] == 'Male')]['Application_Status'] == 'N').mean() * 100

        # Create a pie chart to visualize the percentage
        labels = ['Rejected', 'Not Rejected']
        sizes = [married_males_rejection_percentage, 100 - married_males_rejection_percentage]

        # Plot the data
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
        plt.title('Percentage of Rejection for Married Males')

        # Save the plot as a .png file in the figures folder
        plt.savefig(os.path.join("figures", "percentage_rejection_for_married_males.png"), dpi=300, bbox_inches='tight')
        print(Fore.GREEN + "The Plot Has Been Saved In figures Folder")
        print(Style.RESET_ALL)

        plt.show()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        conn.close()




############################-- percentage rejection for married males --############################
def top_three_months_with_largest_volumeV2():
    

    try:
        # Connect to the MySQL database
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Query the data from the MySQL database
        query = f"SELECT TIMEID, TRANSACTION_VALUE FROM CDW_SAPP_CREDIT_CARD"
        cursor.execute(query)

        # Fetch the results
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        # Create a DataFrame from the query results
        df = pd.DataFrame(results, columns=column_names)

        # Close the database connection
        conn.close()

        # Convert 'TIMEID' to a datetime format (assuming 'TIMEID' is in 'YYYYMMDD' format)
        df['Transaction_Date'] = pd.to_datetime(df['TIMEID'], format='%Y%m%d')

        # Group data by month and calculate the sum of transaction values
        monthly_data = df.groupby(df['Transaction_Date'].dt.to_period("M")).sum(numeric_only=True)

        # Sort the data by transaction value in descending order and select the top three months
        top_three_months = monthly_data.nlargest(3, 'TRANSACTION_VALUE')

        # Plot the top three months
        plt.figure(figsize=(10, 6))
        plt.bar(top_three_months.index.strftime('%Y-%m'), top_three_months['TRANSACTION_VALUE'], width=0.3)
        plt.xlabel('Month')
        plt.ylabel('Transaction Value')
        plt.title('Top Three Months with the Largest Volume of Transaction Data')
        plt.xticks(rotation=45)

        # Show the volume of each of the top three months
        for i, month in enumerate(top_three_months.index.strftime('%Y-%m')):
            volume = top_three_months['TRANSACTION_VALUE'].iloc[i]
            print(f"{month}: ${volume:.2f} Transaction Value")

        # Save the plot as a .png file in the figures folder
        plt.savefig(os.path.join("figures", "top_three_months_with_largest_volume.png"), dpi=300, bbox_inches='tight')
        print(Fore.GREEN + "The Plot Has Been Saved In figures Folder")
        print(Style.RESET_ALL)

        plt.show()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()






############################-- branch with highest healthcare value --############################
def branch_with_highest_healthcare_valueV3():
    # Connect to the MySQL database

    try:
        # Connect to the MySQL database
        conn = connect_to_database()
        cursor = conn.cursor()

        # Query the data from the MySQL database using a SQL JOIN
        query = f"""
            SELECT b.BRANCH_NAME, b.BRANCH_CITY, SUM(t.TRANSACTION_VALUE) AS TOTAL_HEALTHCARE_VALUE
            FROM CDW_SAPP_CREDIT_CARD t
            JOIN CDW_SAPP_BRANCH b ON t.BRANCH_CODE = b.BRANCH_CODE
            WHERE t.TRANSACTION_TYPE = 'Healthcare'
            GROUP BY b.BRANCH_NAME, b.BRANCH_CITY
            ORDER BY TOTAL_HEALTHCARE_VALUE DESC
            LIMIT 1
        """
        cursor.execute(query)
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        # Create a DataFrame from the query results
        df = pd.DataFrame(results, columns=column_names)

        # Adjust the figure size to control the width of the horizontal bars
        plt.figure(figsize=(6, 3))  # You can adjust the figure width and height to control the bar width

        # Plot the branch with the highest total healthcare value as a horizontal bar chart
        plt.barh(df['BRANCH_NAME'], df['TOTAL_HEALTHCARE_VALUE'], 0.1)
        plt.ylabel('Branch City: ' + df['BRANCH_CITY'].iloc[0])
        plt.xlabel('Total Healthcare Transaction Value')
        plt.title('Branch with Highest Total Healthcare Transaction Value')

        # Save the plot as a .png file in the figures folder
        plt.savefig(os.path.join("figures", "branch_with_highest_healthcare_value.png"), dpi=300, bbox_inches='tight')
        print(Fore.GREEN + "The Plot Has Been Saved In figures Folder")
        print(Style.RESET_ALL)

        plt.show()
        return df

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        conn.close()




