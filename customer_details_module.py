import mysql.connector
from app_config import db_config
from util.mysql_connect import connect_to_database
from colorama import Fore, Style
from tqdm import tqdm
from time import sleep

### Used to check the existing account details of a customer
### Used to modify the existing account details of a customer.
### Used to generate a monthly bill for a credit card number for a given month and year.
### Used to display the transactions made by a customer between
#   two dates. Order by year, month, and day in descending order.

# Progress
def progress(r, msg):
    for item in tqdm(range(r), desc=msg, colour="green"):
        sleep(0.05)



############################-- Check account details --############################
def check_account_details():

    customer_ssn = input("Enter Customer SSN: ")

    progress(50, "LOADING...")

    try:
        
        conn = connect_to_database()
        cursor = conn.cursor()

        query = f"SELECT * FROM CDW_SAPP_CUSTOMER WHERE SSN = {customer_ssn}"
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            
            ssn, first_name, middle_name, last_name, credit_card_no, full_street_address, cust_city, cust_state, \
            cust_country, cust_zip, cust_phone, cust_email, last_updated = result

            output = f"First name: {first_name}\nLast name: {last_name}\nSSN: {ssn}\n" \
                     f"Credit Card: {credit_card_no}\nAddress: {full_street_address}, {cust_city}, {cust_state}, " \
                     f"{cust_country}, {cust_zip}\nPhone: {cust_phone}\nEmail: {cust_email}\nLast Updated: {last_updated}"

            print(Fore.GREEN + output)
            print(Style.RESET_ALL)
            
        else:
            print(Fore.RED + "Customer not found")
            print(Style.RESET_ALL)

    except mysql.connector.Error as e:
        print(Fore.CYAN + f"Error: {e}")
        print(Style.RESET_ALL)
        

    finally:
        cursor.close()
        conn.close()






############################-- Modify account details --############################
def modify_account_details():

    customer_ssn = input("Enter Customer SSN: ")

    progress(50, "LOADING...")

    try:
        conn = connect_to_database()
        cursor = conn.cursor()

        # Retrieve the existing customer data
        query = f"SELECT FIRST_NAME, LAST_NAME, FULL_STREET_ADDRESS, CUST_CITY, CUST_STATE, CUST_COUNTRY, CUST_ZIP, CUST_PHONE, CUST_EMAIL FROM CDW_SAPP_CUSTOMER WHERE SSN = {customer_ssn}"
        cursor.execute(query)
        existing_data = cursor.fetchone()

        if not existing_data:
            return "Customer not found"

        # Prompt the user for updated data
        new_data = {}
        new_data['first_name'] = input("Enter new First name: ")
        new_data['last_name'] = input("Enter new Last name: ")
        new_data['address'] = input("Enter new Address: ")
        new_data['city'] = input("Enter new City: ")
        new_data['state'] = input("Enter new State: ")
        new_data['country'] = input("Enter new Country: ")
        new_data['zip'] = input("Enter new ZIP code: ")
        new_data['phone'] = input("Enter new Phone number: ")
        new_data['email'] = input("Enter new Email: ")

        # Define the SQL UPDATE query to modify the customer's data
        update_query = f"UPDATE CDW_SAPP_CUSTOMER SET FIRST_NAME = '{new_data['first_name']}', " \
                      f"LAST_NAME = '{new_data['last_name']}', " \
                      f"FULL_STREET_ADDRESS = '{new_data['address']}', " \
                      f"CUST_CITY = '{new_data['city']}', " \
                      f"CUST_STATE = '{new_data['state']}', " \
                      f"CUST_COUNTRY = '{new_data['country']}', " \
                      f"CUST_ZIP = '{new_data['zip']}', " \
                      f"CUST_PHONE = '{new_data['phone']}', " \
                      f"CUST_EMAIL = '{new_data['email']}' " \
                      f"WHERE SSN = {customer_ssn}"

        cursor.execute(update_query)
        conn.commit()
        

        if cursor.rowcount > 0:
            print(Fore.GREEN + "Account details updated successfully")
            print(Style.RESET_ALL)
        else:
            print(Fore.RED + "No matching customer found")
            print(Style.RESET_ALL)

    except mysql.connector.Error as e:
        print(Fore.CYAN + f"Error: {e}")
        print(Style.RESET_ALL)
        return None

    finally:
        cursor.close()
        conn.close()




############################-- Generate monthly bill --############################
def generate_monthly_bill():

    # Input credit card information
    credit_card_no = input("Enter Credit Card Number: ")
    month = input("Enter Month (MM): ")
    year = input("Enter Year (YYYY): ")

    # Display a loading bar
    progress(50, "LOADING...")

    try:
        # Connect to the database
        conn = connect_to_database()
        cursor = conn.cursor()

        # SQL query to calculate the total bill for the specified month and year
        query = f"SELECT SUM(TRANSACTION_VALUE) FROM CDW_SAPP_CREDIT_CARD WHERE CUST_CC_NO = {credit_card_no} AND " \
                f"YEAR(TIMEID) = {year} AND MONTH(TIMEID) = {month}"

        cursor.execute(query)
        result = cursor.fetchone()

        if result and result[0]:
            # Print the total bill if transactions are found
            print(Fore.GREEN + f"Total bill for {month}/{year}: ${result[0]}")
            print(Style.RESET_ALL)
        else:
            # Print a message if no transactions are found
            print(Fore.RED + "No transactions found for the specified month and year")
            print(Style.RESET_ALL)

    except mysql.connector.Error as e:
        # Handle database errors
        print(Fore.CYAN + f"Error: {e}")
        print(Style.RESET_ALL)
        return None

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()





############################-- Display transactions --############################
def display_transactions():

    # Input customer information
    customer_ssn = input("Enter Customer SSN: ")
    start_date = input("Enter Start Date (YYYYMMDD): ")
    end_date = input("Enter End Date (YYYYMMDD): ")

    # Display a loading bar
    progress(50, "LOADING...")

    try:
        # Connect to the database
        conn = connect_to_database()
        cursor = conn.cursor()

        # SQL query to fetch transactions
        query = f"SELECT * FROM CDW_SAPP_CREDIT_CARD WHERE CUST_SSN = {customer_ssn} " \
                f"AND TIMEID BETWEEN {start_date} AND {end_date} ORDER BY TIMEID DESC"

        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            return "No transactions found within the specified date range"

        formatted_results = []
        # Format and store transaction results
        for result in results:
            card_number, time_id, cust_ssn, branch_code, transaction_type, transaction_value, transaction_id = result

            formatted_result = {
                'Card Number': card_number,
                'Transaction Date': time_id,
                'Customer SSN': cust_ssn,
                'Branch Code': branch_code,
                'Transaction Type': transaction_type,
                'Transaction Value': f"${transaction_value}",
                'Transaction ID': transaction_id
            }
            formatted_results.append(formatted_result)

        if formatted_results == "No transactions found within the specified date range":
            # Print message if no transactions found
            print(Fore.GREEN + formatted_results)
            print(Style.RESET_ALL)
        else:
            for transaction in formatted_results:
                # Print transaction details
                print(Fore.BLACK + "Transaction Details:")
                
                for key, value in transaction.items():
                    print(Fore.GREEN + f"{key}: {value}")
                    print("-" * 30)
                    
                print("\n")
                print(Style.RESET_ALL)

    except mysql.connector.Error as e:
        # Handle database errors
        print(Fore.CYAN + f"Error: {e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

