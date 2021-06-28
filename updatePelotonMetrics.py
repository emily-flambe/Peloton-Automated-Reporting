import datetime
import time
import gspread
from gspread_dataframe import set_with_dataframe
import json
import os
import pandas as pd
import requests

username = os.getenv("PELOTON_USERNAME")
password = os.getenv("PELOTON_PASSWORD")
user_id = os.getenv("PELOTON_USER_ID")
service_account_creds = os.getenv("SERVICE_ACCOUNT_CREDS")
worksheet_key = os.getenv("WORKSHEET_KEY")
sheet_index = int(os.getenv("SHEET_INDEX"))

KEYFILE = 'service_account_creds.json'

def convertEpochToTimestamp(epoch):
    date_format = "%Y-%m-%dT%H:%m:%S"
    created_at_datetime = datetime.datetime.fromtimestamp(epoch)
    created_at_date = created_at_datetime.strftime(date_format)
    return created_at_date

def authenticate():
    # start requests session & authenticate
    s = requests.Session()
    payload = {'username_or_email': username, 'password': password}
    s.post('https://api.onepeloton.com/auth/login', json=payload)

    return s


def getWorkouts(s):
    # get workouts
    route = f"https://api.onepeloton.com/api/user/{user_id}/workouts?limit=99999"
    my_results = s.get(route).json()
    my_results_df = pd.DataFrame(my_results['data'])
    my_results_df['created_at_timestamp'] = [convertEpochToTimestamp(x) for x in my_results_df['created_at']]
    my_results_df['start_time_timestamp'] = [convertEpochToTimestamp(x) for x in my_results_df['start_time']]
    my_results_df['end_time_timestamp'] = [convertEpochToTimestamp(x) for x in my_results_df['end_time']]

    my_results_df = my_results_df[[
        'created_at_timestamp'
        , 'start_time_timestamp'
        , 'end_time_timestamp'
        , 'id'
        , 'is_total_work_personal_record'
        , 'status'
        , 'total_work']]

    # filter to workouts on or after April 1, 2021
    # my_results_df = my_results_df[my_results_df['created_at']>=1617458062]

    return my_results_df


def getWorkoutDetails(s, workouts_df):
    workout_ids = workouts_df['id']

    workout_data_fields = [
        'description'
        , 'difficulty_rating_avg'
        , 'duration'
        , 'id'
        , 'image_url'
        , 'instructor_id'
        , 'is_explicit'
        , 'length'
        , 'location'
        , 'overall_rating_avg'
        , 'overall_rating_count'
        , 'ride_type_id'
        , 'series_id'
        , 'title'
    ]

    base_uri = "https://api.onepeloton.com/api/workout/"
    workouts_dict_list = []

    i = 1
    total = len(workout_ids)
    for workout_id in workout_ids:
        print(f"Processing workout {i} out of {total}...")
        route = base_uri + workout_id
        workout_details = s.get(route).json()
        workout_details_dict = {k: v for k, v in workout_details['ride'].items() if k in workout_data_fields}
        workout_details_dict['workout_id'] = workout_id
        workouts_dict_list.append(workout_details_dict)
        i += 1

    workout_df = pd.DataFrame(workouts_dict_list)

    workout_df = workout_df.rename(columns={"id": "ride_id", "workout_id": "id"})

    return workout_df


def getAllInstructors(s):
    '''
    Returns a dataframe relating instructor_id to their full names
    '''
    instructors = s.get("https://api.onepeloton.com/api/instructor/?limit=200").json()['data']
    instructors_df = pd.DataFrame(instructors)
    instructors_df['instructor_name'] = instructors_df['first_name'] + ' ' + instructors_df['last_name']
    instructors_df = instructors_df.rename(columns={"id": "instructor_id"})

    return instructors_df[['instructor_id', 'instructor_name']]


def updateGoogleSheet(worksheet_key, sheet_index, df):
    '''
    Returns Google Sheets `worksheet` object
    '''

    # Authenticate
    try:
        with open(KEYFILE, "w") as secret_file:
            secret_file.write(service_account_creds)
        gc = gspread.service_account(filename=KEYFILE)
        print("Authentication succeeded, yaaay")
        os.remove(KEYFILE)

    except:
        print("Google Sheets authentication failed :( :( :(")
        
    # Open the google worksheet
    sh = gc.open_by_key(worksheet_key)
    worksheet = sh.get_worksheet(sheet_index)  # -> 0 - first sheet, 1 - second sheet etc. 

    # Update the data in the google sheet
    set_with_dataframe(worksheet, df)  # -> THIS EXPORTS YOUR DATAFRAME TO THE GOOGLE SHEET

def updateMetrics():
    s = authenticate()

    # Get workout summaries
    print("Fetching workout summary data...")
    workouts_df = getWorkouts(s)

    # Get workout details and join to summaries
    print("Fetching workout details...")
    workout_details_df = getWorkoutDetails(s, workouts_df)
    combined_df = workouts_df.merge(workout_details_df, how='inner', on='id')

    # Join instructor names to workouts
    print("Fetching instructor details...")
    instructor_details_df = getAllInstructors(s)
    combined_df = combined_df.merge(instructor_details_df, how='inner', on='instructor_id')

    # Update the data in the Google Sheet with the new data
    print("Updating data in Google Sheets...")
    updateGoogleSheet(worksheet_key, sheet_index, combined_df)

    print("All done! Enjoy your fresh data")


def main():

    INTERVAL = 60 * 60 * 6  # run every 6 hours

    while True:

        updateMetrics()

        # back to sleep zzzzzz
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()  