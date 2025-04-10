import pandas as pan # Pandas to process the downloaded .csv file into a list of IDs
from concurrent.futures import ThreadPoolExecutor, as_completed # Threading libarary to process mutiple boardgames at the same time 
from threading import Lock # A thread lock for the progrees bar function and for updating the processed_ids.csv file
from etl_utils import process_board_game, nuke_database # Custom functions from a seperate file for organizational purposes
from time import perf_counter # A timer to assist in displaying the ETA and average time in the progress bar function
import os # For updating the processed_ids.csv file
import traceback # For printing full errors to debug

ID_FILE = 'id_list.csv' # The list of boardgame IDs taken from boardgames_ranks.csv 
PROCESSED_FILE = 'processed_ids.csv' # A file store processed ID's to save progress if the script crashes

# Take the ID's from the boardgames_ranks.csv and convert it to a list in the ID_FILE 
full_df = pan.read_csv('boardgames_ranks.csv')
id_df = full_df[['id']]
id_df.to_csv(ID_FILE, index=False)
id_df = pan.read_csv(ID_FILE)
all_ids = set(id_df['id'].tolist())

# Read processed_ids.csv file
if os.path.exists(PROCESSED_FILE):
    processed_ids = set(pan.read_csv(PROCESSED_FILE)['id'].tolist())
else:
    processed_ids = set()

# get the remaining IDs by taking the ID list and subtracting the processed IDs
ids = list(all_ids - processed_ids)

progress_lock = Lock() # A thread lock for the print_progress_bar function
file_lock = Lock() # A thread lock for the processed_ids.csv file
completed = 0 # A variable passed to print_progress_bar function to track how many has been completed
total = len(ids) # Total amount of ids to be processed passed to the print_progress_bar function
max_workers = 1 # Total amount of threads allowed to run at any time typically 1-10
total_time_elapsed = 0 # The total amount of time elapsed, used to calculate ETA and average in the print_progress_bar function

# A debug function to DELETE EVERYTHING in the database for a fresh start
# THIS SHOULD BE COMMENTED OUT ON A NORMAL RUN
### nuke_database() ###

# Formats the time for the print_progress_bar function into - Days, Hours:Minutes:Seconds
def format_timedelta(seconds):
    total_seconds = round(seconds)
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, sec = divmod(remainder, 60)

    if days > 0:
        return f"{days} days, {hours}:{minutes:02}:{sec:02}"
    else:
        return f"{hours}:{minutes:02}:{sec:02}"

# Print a progress bar, percentage finished, processed/total, and ETA, and the average time to process one boardgame
def print_progress_bar(completed, total, bar_length=50):
    global last_time, total_time_elapsed

    now = perf_counter() # Stop the timer
    elapsed = now - last_time # Get the time elapsed since the last thread was completed
    last_time = now # Reset the timer 

    total_time_elapsed += elapsed # Add elapsed time to the running total
    avg_time = total_time_elapsed / completed # Calculate the average time it takes to process a board game
    time_per_board_game = f"{avg_time:.2f}s" # Format average to be in seconds with 2 decimal places

    remaining = total - completed # The amount of remaining board games, there are to be processed
    eta = format_timedelta(avg_time * remaining) # Calulate the estimated time remaining and format it
    

    # Build progress bar
    progress = completed / total
    block = int(bar_length * progress)
    bar = "#" * block + "-" * (bar_length - block)
    percentage = progress * 100

    print("\r" + " " * 120, end='')  # Clear previous line
    print(f"\r[{bar}] {percentage:.2f}% {completed}/{total} • ETA: {eta} • avg: {time_per_board_game}", end='', flush=True)

# Start the timer for the first thread's ETA calculation
last_time = perf_counter()

# Run the process_board_game with threads, run through all ids in the list
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_id = {executor.submit(process_board_game, id): id for id in ids}

    for future in as_completed(future_to_id):
        id = future_to_id[future]
        try:
            result = future.result()
            # If sucessful, save the id to the processed_ids.csv file
            with file_lock: 
                try:
                    with open(PROCESSED_FILE, 'a') as f:
                        if os.stat(PROCESSED_FILE).st_size == 0:
                            f.write("id\n")
                        f.write(f"{id}\n")
                except Exception as e:
                    print(f"⚠️ Failed to write to {PROCESSED_FILE}: {e}")
        except Exception as e:
            # If the error contains deadlock victim, it means it failed to write to the sql server due to it competing with other threads
            if "deadlock victim" in str(e).lower():
                print(f"\nDeadlock victim error encountered for ID: {id}")
            # The thread went too fast and is trying to add to a table using a board_game_id that is still being processed
            elif "foreign key" in str(e).lower():
                print(f"\nForeign key constraint error for ID: {id}")
            # If it is not a deadlock victim or foriegn key error print out full error to debug 
            else:
                print(f"\nError processing ID {id} Error {e}")
                traceback.print_exc()
        # Success or failure, update the completed total and print the progress bar
        # Note: If there is a error the id is not added to the processed IDs, so they will be re processed on future runs
        with progress_lock:
            completed += 1
            print_progress_bar(completed, total)

print("\n✅ Finished processing all IDs!!!")