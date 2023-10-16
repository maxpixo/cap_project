from util.mysql_connect import connect_to_database
import customer_details_module 
import data_analysis
from colorama import Fore,Style
import transaction_details_module
import api_analysis

def transaction_details_module_menu():
    while True:
        print("Transaction Details Module")
        print("1. Display transactions by zip code and month/year")
        print("2. Display number and total values of transactions by type")
        print("3. Display total number and total values of transactions by state")
        print("4. Back to Main Menu")
        
        choice = input("Enter your choice: ")
        
        if choice == '1':
            # Function for displaying transactions by zip code and month/year
            transaction_details_module.display_transactions_by_zip()
            pass
        elif choice == '2':
            # Function for displaying number and total values of transactions by type
            transaction_details_module.display_transactions_by_type()
            pass
        elif choice == '3':
            # Function for displaying total number and total values of transactions by state
            transaction_details_module.display_transactions_by_state()
            pass
        elif choice == '4':
            break
        else:
            print("Invalid choice. Please try again.")

def customer_details_module_menu():
    while True:
        print("Customer Details Module")
        print("1. Check existing account details")
        print("2. Modify account details")
        print("3. Generate monthly bill")
        print("4. Display transactions between two dates")
        print("5. Back to Main Menu")
        
        choice = input("Enter your choice: ")
        
        if choice == '1':
            # Function for checking existing account details
            customer_details_module.check_account_details()
            pass
        elif choice == '2':
            # Function for modifying account details
            customer_details_module.modify_account_details()
            pass
        elif choice == '3':
            # Function for generating a monthly bill
            customer_details_module.generate_monthly_bill()
            pass
        elif choice == '4':
            # Function for displaying transactions between two dates
            customer_details_module.display_transactions()
            pass
        elif choice == '5':
            break
        else:
            print("Invalid choice. Please try again.")

def data_analysis_module_menu():
    while True:
        print("Data Analysis and Visualization")
        print("1. Find and plot highest transaction count by type")
        print("2. Find and plot state with a high number of customers")
        print("3. Find and plot total transactions for top 10 customers")
        print("4. Back to Main Menu")
        
        choice = input("Enter your choice: ")
        
        if choice == '1':
            # Function for finding and plotting highest transaction count by type
            data_analysis.find_highest_transaction_count()
            pass
        elif choice == '2':
            # Function for finding and plotting state with a high number of customers
            data_analysis.find_state_with_high_customers()
            pass
        elif choice == '3':
            # Function for finding and plotting total transactions for top 10 customers
            data_analysis.find_top_10_customers()
            pass
        elif choice == '4':
            break
        else:
            print("Invalid choice. Please try again.")

def api_analysis_module_menu():
    while True:
        print("API Analysis Module")
        print("1. Find and plot percentage of approved applications for self-employed")
        print("2. Find percentage of rejection for married male applicants")
        print("3. Find and plot top three months with the largest transaction volume")
        print("4. Find and plot branch with the highest total dollar value of healthcare transactions")
        print("5. Back to Main Menu")
        
        choice = input("Enter your choice: ")
        
        if choice == '1':
            # Function for finding and plotting percentage of approved applications for self-employed
            api_analysis.percentage_approved_for_self_employedV2()
            pass
        elif choice == '2':
            # Function for finding percentage of rejection for married male applicants
            api_analysis.percentage_rejection_for_married_malesV2()
            pass
        elif choice == '3':
            # Function for finding and plotting top three months with the largest transaction volume
            api_analysis.top_three_months_with_largest_volumeV2()
            pass
        elif choice == '4':
            # Function for finding and plotting branch with the highest total dollar value of healthcare transactions
            api_analysis.branch_with_highest_healthcare_valueV3()
            pass
        elif choice == '5':
            break
        else:
            print("Invalid choice. Please try again.")

def main_menu():
    while True:
        print(Fore.GREEN + "Main Menu:")
        print("1. Transaction Details Module")
        print("2. Customer Details Module")
        print("3. Data Analysis and Visualization")
        print("4. API Analysis Module")
        print("5. Exit")
        
        choice = input("Enter your choice: ")
        
        if choice == '1':
            transaction_details_module_menu()
        elif choice == '2':
            customer_details_module_menu()
        elif choice == '3':
            data_analysis_module_menu()
        elif choice == '4':
            api_analysis_module_menu()
        elif choice == '5':
            print("Exiting the program. Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")


