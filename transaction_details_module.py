
from util.mysql_connect import connect_to_database
import mysql
from datetime import datetime
from colorama import Fore, Style
from tqdm import tqdm
from time import sleep

def progress(r, msg):
    for item in tqdm(range(r), desc=msg, colour="green"):
        sleep(0.05)


# Convert the original date format (YYYYMMDD) to a datetime object
def format_date(date_str):
    
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    # Format the date as MM/DD/YYYY
    formatted_date = date_obj.strftime("%m/%d/%Y")
    return formatted_date


# Task 1: Display transactions by zip code and month/year
def display_transactions_by_zip():

    zipcode = input("Enter Zip Code: ")
    month = input("Enter Month (MM): ")
    year = input("Enter Year (YYYY): ")

    progress(50,Fore.GREEN + "LOADING...")

    

    try:
        connection = connect_to_database()
        cursor = connection.cursor()
        query = """
            SELECT
                t.CUST_CC_NO,
                t.TIMEID,
                t.BRANCH_CODE,
                t.TRANSACTION_TYPE,
                t.TRANSACTION_VALUE
            FROM
                CDW_SAPP_CREDIT_CARD t
            JOIN
                CDW_SAPP_CUSTOMER e
            ON
                t.CUST_CC_NO = e.Credit_card_no
            WHERE
                e.CUST_ZIP = %s
                AND MONTH(t.TIMEID) = %s
                AND YEAR(t.TIMEID) = %s
            ORDER BY
                DAY(t.TIMEID) DESC;
        """
        cursor.execute(query, (zipcode, month, year))
        transactions = cursor.fetchall()

        

        if transactions:
            print(Fore.BLACK + "Transactions by Zip Code, Month, and Year:")
            print(Style.RESET_ALL)
            for transaction in transactions:
                print(Fore.GREEN + "Credit Card Number:", transaction[0])
                print(Fore.GREEN + "Transaction Date:", format_date(transaction[1]))
                print(Fore.GREEN + "Branch Code:", transaction[2])
                print(Fore.GREEN + "Transaction Type:", transaction[3])
                print(Fore.GREEN + f"Transaction Value:", f"${transaction[4]}")
                print(Fore.BLACK + "-" * 30)
            print(Style.RESET_ALL)
        else:
            print(Fore.RED + "No transactions found for the specified criteria.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        connection.close()




# Task 2: Display number and total values of transactions by type
def display_transactions_by_type():

    transaction_type = input("Enter transaction type: ")

    progress(50,Fore.GREEN + "LOADING...")


    try:
        connection = connect_to_database()
        cursor = connection.cursor()
        query = """
            SELECT COUNT(*), ROUND(SUM(TRANSACTION_VALUE), 2)
            FROM CDW_SAPP_CREDIT_CARD
            WHERE TRANSACTION_TYPE = %s;
        """
        cursor.execute(query, (transaction_type,))
        result = cursor.fetchone()

        if result:
            count, total_value = result
            print(Fore.GREEN + f"Number of '{transaction_type}' transactions: {count}")
            print(Fore.GREEN + f"Total value of '{transaction_type}' transactions: ${total_value}")
            print(Style.RESET_ALL)
        else:
            print(Fore.RED + f"No '{transaction_type}' transactions found.")
            print(Style.RESET_ALL)

    except mysql.connector.Error as err:
        print(Fore.CYAN + f"Error: {err}")
        print(Style.RESET_ALL)

    finally:
        cursor.close()
        connection.close()    



# Task 3: Display total number and total values of transactions by state
def display_transactions_by_state():

    state = input("Enter state: ")

    progress(50,Fore.GREEN + "LOADING...")

    
    try:
        connection = connect_to_database()
        cursor = connection.cursor()
        query = """
            SELECT COUNT(*), ROUND(SUM(TRANSACTION_VALUE), 2)
            FROM CDW_SAPP_CREDIT_CARD AS t
            INNER JOIN CDW_SAPP_BRANCH AS b ON t.BRANCH_CODE = b.BRANCH_CODE
            WHERE b.BRANCH_STATE = %s;
        """
        cursor.execute(query, (state,))
        result = cursor.fetchone()

        if result:
            count, total_value = result
            print(Fore.GREEN + f"Total number of transactions in '{state}': {count}")
            print(Fore.GREEN + f"Total value of transactions in '{state}': ${total_value}")
            print(Style.RESET_ALL)
        else:
            print(Fore.GREEN + f"No transactions found in '{state}'.")
            print(Style.RESET_ALL)

    except mysql.connector.Error as err:
        print(Fore.GREEN + f"Error: {err}")
        print(Style.RESET_ALL)
    finally:
        cursor.close()
        connection.close()



