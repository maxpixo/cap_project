
import os
from time import sleep
from tqdm import tqdm
import etl
import menu
from colorama import Fore, Style




# Progress Bar
def progress(r, msg):
    for item in tqdm(range(r), desc=msg, colour="green"):
        sleep(0.05)


def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')



if __name__ == "__main__":

   
    etl.etl_etl()

    # Show Progress Bar
    progress(50, "Loading...")

    
    # Clear Screen
    clear_screen()

    

    print(Fore.GREEN + "████████████████████████████████  DATA EXTRACTED         ████████████████████████████████")
    print(Fore.GREEN + "████████████████████████████████  DATA TRANSFORMED       ████████████████████████████████")
    print(Fore.GREEN + "████████████████████████████████  DATA LOADED            ████████████████████████████████")
    print(Fore.GREEN + "████████████████████████████████  Welcome to App         ████████████████████████████████")
    print(Style.RESET_ALL)
    menu.main_menu()
    

    