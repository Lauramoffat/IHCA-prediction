import pandas as pd
from datetime import datetime

DATA_DIRECTORY = r'J:\mimic-iii-clinical-database-1.4'
CHUNK_SIZE = 1000000

# Hardcoded ITEMID
HEART_RATE_METAVISION_ID = 220045
AORTIC_PRESSURE_SIGNAL_METAVISION_ID = 228152
MANUAL_BLOOD_PRESSURE_SYS_LEFT_METAVISION_ID = 224167
MANUAL_BLOOD_PRESSURE_SYS_RIGHT_METAVISION_ID = 227243
RV_SYS_PRESSURE_METAVISION_ID = 226850
PA_SYS_PRESSURE_METAVISION_ID = 226852
ARTERIAL_BLOOD_PRESSURE_SYS_METAVISION_ID = 220050
PULMONARY_BLOOD_PRESSURE_SYS_METAVISION_ID = 220059
NON_INVASIVE_BLOOD_PRESSURE_SYS_METAVISION_ID = 220179
ART_BP_SYS_METAVISION_ID = 225309
RESPIRATORY_RATE_SET_METAVISION_ID = 224688
RESPIRATORY_RATE_SPONTANEOUS_METAVISION_ID = 224689
RESPIRATORY_RATE_TOTAL_METAVISION_ID = 224690
RESPIRATORY_RATE_METAVISION_ID = 220210
SPO2_METAVISION_ID = 220277


def parse_mimic_data():
    raw_data_df = pd.DataFrame()    # DataFrame to hold raw data for NN (gender, age, heart rate, etc.)
    chart_events_df = pd.read_csv(DATA_DIRECTORY + "\CHARTEVENTS.csv", chunksize=CHUNK_SIZE)
    patient_df = pd.read_csv(DATA_DIRECTORY + "\PATIENTS.csv")
    admissions_df = pd.read_csv(DATA_DIRECTORY + "\ADMISSIONS.csv")

    id_match_arr = [
        HEART_RATE_METAVISION_ID,
        AORTIC_PRESSURE_SIGNAL_METAVISION_ID,
        MANUAL_BLOOD_PRESSURE_SYS_LEFT_METAVISION_ID,
        MANUAL_BLOOD_PRESSURE_SYS_RIGHT_METAVISION_ID,
        RV_SYS_PRESSURE_METAVISION_ID,
        PA_SYS_PRESSURE_METAVISION_ID,
        ARTERIAL_BLOOD_PRESSURE_SYS_METAVISION_ID,
        PULMONARY_BLOOD_PRESSURE_SYS_METAVISION_ID,
        NON_INVASIVE_BLOOD_PRESSURE_SYS_METAVISION_ID,
        ART_BP_SYS_METAVISION_ID,
        RESPIRATORY_RATE_SET_METAVISION_ID,
        RESPIRATORY_RATE_SPONTANEOUS_METAVISION_ID,
        RESPIRATORY_RATE_TOTAL_METAVISION_ID,
        RESPIRATORY_RATE_METAVISION_ID,
        SPO2_METAVISION_ID
    ]

    # Iterate over each "chunk" of data in the chartevents table, and search for specific itemids
    for chunk in chart_events_df:
        # Iterate over each desired ITEMID
        for item_id in id_match_arr:
            match_df = chunk[chunk['ITEMID'] == item_id]
            # Iterate over each row of the matching dataframe, and obtain patient gender/age
            for index, row in match_df.iterrows():
                # Find the matching patient information from the PATIENTS table
                patient_data = patient_df[patient_df['SUBJECT_ID'] == row['SUBJECT_ID']]

                # Extract desired patient characteristics
                gender = patient_data['GENDER'].iloc[0]
                dob_string = patient_data['DOB'].iloc[0]
                dob_timestamp = datetime.strptime(dob_string, '%Y-%m-%d %H:%M:%S')

                # Age is calculated as the difference between the date of birth (PATIENTS table) and first admission
                # time (ADMISSIONS table)
                matched_admissions = admissions_df[admissions_df['SUBJECT_ID'] == row['SUBJECT_ID']]
                first_admission_time_string = matched_admissions.iloc[0]['ADMITTIME']
                first_admission_timestamp = datetime.strptime(first_admission_time_string, '%m/%d/%Y %H:%M')

                age = (first_admission_timestamp - dob_timestamp).days / 365
                if age >= 300:
                    age = 89

                print()
        print()
    print()