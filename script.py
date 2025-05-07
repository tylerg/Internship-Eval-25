import pandas as pd
import numpy as np
import os
import glob
import tarfile
import io # For reading files from tar in memory
import collections # Added for mode calculation
# No need for google.colab import drive or drive.mount() when running locally

# --- Global Configuration & Data Structures ---
MAIN_FOLDER_PATH = r"C:\Users\creep\OneDrive\Documents\Internship Eval 25\synthea_1m_fhir_3_0_May_24"

CKD_DATE_RANGE_START = pd.to_datetime('1997-01-01')
CKD_DATE_RANGE_END = pd.to_datetime('2023-12-31')

# SNOMED CT codes for CKD and related conditions
ckd_snomed_codes = [
    '431855005', # Chronic kidney disease stage 1 (disorder)
    '431856006', # Chronic kidney disease stage 2 (disorder)
    '433144002', # Chronic kidney disease stage 3 (disorder)
    '431857002', # Chronic kidney disease stage 4 (disorder)
    '433146000', # Chronic kidney disease stage 5 (disorder)
    '709044004', # Chronic kidney disease (disorder) - General
    '714153000', # Chronic kidney disease stage 5 with transplant (disorder)
    '714152005', # Chronic kidney disease stage 5 on dialysis (disorder)
    '713313000', # Chronic kidney disease mineral and bone disorder (disorder)
    '722149000', # Chronic kidney disease due to and following excision of neoplasm of kidney (disorder)
    '726018006', # Autosomal dominant tubulointerstitial kidney disease (disorder)
    '723373006'  # Uromodulin related autosomal dominant tubulointerstitial kidney disease (disorder)
]

# Function to map SNOMED codes to CKD stages
def map_code_to_stage(code_val):
    code_str = str(code_val)
    if code_str == '431855005': return 1
    elif code_str == '431856006': return 2
    elif code_str == '433144002': return 3
    elif code_str == '431857002': return 4
    elif code_str in ['433146000', '714153000']: return 5 # Stage 5 or Stage 5 with transplant
    elif code_str == '714152005': return 6 # ESRD on dialysis
    elif code_str == '709044004': return 0 # General CKD (excluding from stage analysis but included in patient count)
    return np.nan # Ignore codes not mapping to a specific stage or general CKD

# Stage transitions to analyze
stage_transitions = {
    'Stage 1 to Stage 2': (1, 2),
    'Stage 2 to Stage 3': (2, 3),
    'Stage 3 to Stage 4': (3, 4),
    'Stage 4 to Stage 5': (4, 5),
    'Stage 5 to End Stage Renal Disease': (5, 6) # Using stage 6 for ESRD
}

# Global accumulators
global_patient_progression_details = {} # {patient_id: {'diagnoses': {stage: earliest_date}}}
all_ckd_patient_ids_overall_set = set() # Set of all patient IDs with any CKD code (including stage 0)

# --- Main Processing Loop ---
# Use os.path.join for cross-platform compatibility (though backslashes work on Windows)
archive_files = glob.glob(os.path.join(MAIN_FOLDER_PATH, "*.tar.gz")) # Simpler pattern
if not archive_files:
    print(f"No '*.tar.gz' files found in {MAIN_FOLDER_PATH}")
else:
    print(f"Found {len(archive_files)} archive files to process.")

for archive_path in archive_files:
    print(f"\nProcessing archive: {os.path.basename(archive_path)}")
    try:
        with tarfile.open(archive_path, "r:gz") as tar:
            patients_csv_path_in_tar = None
            conditions_csv_path_in_tar = None

            # Flexible search for csv files within the tar archive
            # Synthea >= 3.0 puts these in a 'csv' folder
            # Synthea < 3.0 might have them directly in the tar
            found_patients = False
            found_conditions = False
            for member in tar.getmembers():
                if member.isfile(): # Ensure it's a file
                    # Check for paths like 'csv/patients.csv' or just 'patients.csv'
                    if member.name.lower().endswith("/patients.csv") or os.path.basename(member.name).lower() == "patients.csv":
                        patients_csv_path_in_tar = member.name
                        found_patients = True
                    # Check for paths like 'csv/conditions.csv' or just 'conditions.csv'
                    elif member.name.lower().endswith("/conditions.csv") or os.path.basename(member.name).lower() == "conditions.csv":
                         conditions_csv_path_in_tar = member.name
                         found_conditions = True
                # Optimization: Stop searching once both are found
                if found_patients and found_conditions:
                     break


            if not patients_csv_path_in_tar:
                print(f"  - Could not find 'patients.csv' in {os.path.basename(archive_path)}")
                continue
            if not conditions_csv_path_in_tar:
                print(f"  - Could not find 'conditions.csv' in {os.path.basename(archive_path)}")
                continue

            print(f"  - Found patients.csv at: {patients_csv_path_in_tar}")
            print(f"  - Found conditions.csv at: {conditions_csv_path_in_tar}")

            # Extract and read patients.csv
            # Using a context manager for file objects is safer
            with tar.extractfile(patients_csv_path_in_tar) as patients_file_obj:
                 if not patients_file_obj: continue
                 # Read into BytesIO first for pandas to handle
                 patients_data = io.BytesIO(patients_file_obj.read())
                 current_patients_df = pd.read_csv(patients_data, on_bad_lines='skip', low_memory=False)


            # Extract and read conditions.csv
            with tar.extractfile(conditions_csv_path_in_tar) as conditions_file_obj:
                if not conditions_file_obj: continue
                # Read into BytesIO first for pandas to handle
                conditions_data = io.BytesIO(conditions_file_obj.read())
                current_conditions_df = pd.read_csv(conditions_data, on_bad_lines='skip', low_memory=False)


            # 1. Data Cleaning and Formatting (for current DFs)
            # Only convert date columns that exist and are needed
            if 'BIRTHDATE' in current_patients_df.columns:
                current_patients_df['BIRTHDATE'] = pd.to_datetime(current_patients_df['BIRTHDATE'], errors='coerce')
            if 'DEATHDATE' in current_patients_df.columns:
                current_patients_df['DEATHDATE'] = pd.to_datetime(current_patients_df['DEATHDATE'], errors='coerce')

            if 'START' in current_conditions_df.columns:
                 current_conditions_df['START'] = pd.to_datetime(current_conditions_df['START'], errors='coerce')
            if 'STOP' in current_conditions_df.columns:
                 current_conditions_df['STOP'] = pd.to_datetime(current_conditions_df['STOP'], errors='coerce')

            # Filter conditions by date range
            if 'START' in current_conditions_df.columns:
                 current_conditions_df = current_conditions_df[
                     (current_conditions_df['START'] >= CKD_DATE_RANGE_START) &
                     (current_conditions_df['START'] <= CKD_DATE_RANGE_END)
                 ].copy() # Use .copy() to avoid SettingWithCopyWarning


            # 2. Identify Patients with CKD (for current DFs)
            if 'CODE' in current_conditions_df.columns and 'PATIENT' in current_conditions_df.columns:
                current_conditions_df['CODE'] = current_conditions_df['CODE'].astype(str)
                # Filter for relevant CKD codes
                temp_ckd_conditions_df = current_conditions_df[current_conditions_df['CODE'].isin(ckd_snomed_codes)].copy()

                if temp_ckd_conditions_df.empty:
                    print(f"  - No initial CKD-coded conditions found in {os.path.basename(archive_path)} within date range.")
                    continue

                current_archive_ckd_patient_ids = temp_ckd_conditions_df['PATIENT'].unique()
                all_ckd_patient_ids_overall_set.update(current_archive_ckd_patient_ids)

                # 3. Map to Stages and Filter (for current DFs)
                temp_ckd_conditions_df.loc[:, 'CKD_STAGE'] = temp_ckd_conditions_df['CODE'].apply(map_code_to_stage)
                # Keep only conditions that map to a specific stage (0-6)
                temp_ckd_conditions_df.dropna(subset=['CKD_STAGE'], inplace=True)

                # Filter out stage 0 (general CKD) for direct progression analysis
                current_staged_conditions_df = temp_ckd_conditions_df[temp_ckd_conditions_df['CKD_STAGE'] != 0].copy()

                if current_staged_conditions_df.empty:
                    print(f"  - No conditions mapping to specific CKD stages (1-6) in {os.path.basename(archive_path)}.")
                    continue

                current_staged_conditions_df.loc[:, 'CKD_STAGE'] = current_staged_conditions_df['CKD_STAGE'].astype(int)
                current_staged_conditions_df = current_staged_conditions_df.sort_values(by=['PATIENT', 'START'])

                # --- Aggregate into global_patient_progression_details ---
                # Iterate through unique patient IDs found in THIS archive's STAGED CKD data
                for patient_id in current_staged_conditions_df['PATIENT'].unique():
                    patient_data_in_file = current_staged_conditions_df[current_staged_conditions_df['PATIENT'] == patient_id]

                    # Consolidate: get earliest date for highest stage if multiple on same day
                    # Sort by date first, then stage descending for ties on the same date
                    patient_data_in_file = patient_data_in_file.sort_values(by=['START', 'CKD_STAGE'], ascending=[True, False])
                    # Keep only the first occurrence for each stage (which will be the earliest date)
                    patient_data_in_file = patient_data_in_file.drop_duplicates(subset=['CKD_STAGE'], keep='first')
                    # Sort by date again just for clarity
                    patient_data_in_file = patient_data_in_file.sort_values('START')


                    temp_stages_for_patient_this_file = {}
                    for _, row in patient_data_in_file.iterrows():
                        diagnosed_stage = row['CKD_STAGE']
                        diagnosis_date = row['START']
                         # Record earliest date for this stage from this file
                         # This check isn't strictly necessary after drop_duplicates(keep='first') above,
                         # but doesn't hurt and makes the logic clearer for merging across files.
                        if diagnosed_stage not in temp_stages_for_patient_this_file or \
                           diagnosis_date < temp_stages_for_patient_this_file[diagnosed_stage]:
                            temp_stages_for_patient_this_file[diagnosed_stage] = diagnosis_date

                    if not temp_stages_for_patient_this_file:
                         continue

                    # Merge with global data for this patient, keeping the absolute earliest date for each stage
                    if patient_id not in global_patient_progression_details:
                        global_patient_progression_details[patient_id] = {'diagnoses': temp_stages_for_patient_this_file}
                    else:
                        for stage, date_val in temp_stages_for_patient_this_file.items():
                            if stage not in global_patient_progression_details[patient_id]['diagnoses'] or \
                               date_val < global_patient_progression_details[patient_id]['diagnoses'][stage]:
                                global_patient_progression_details[patient_id]['diagnoses'][stage] = date_val
                print(f"  - Processed and aggregated data from {os.path.basename(archive_path)}")
            else:
                print(f"  - Missing 'CODE' or 'PATIENT' column in conditions.csv for {os.path.basename(archive_path)}")


    except FileNotFoundError:
        print(f"  - Archive not found during open: {archive_path}")
    except tarfile.ReadError:
        print(f"  - Error reading tar archive: {archive_path}")
    except Exception as e:
        print(f"  - An unexpected error occurred with archive {archive_path}: {e}")


# --- Post-Loop Analysis (using globally aggregated data) ---
print("\n--- Performing final analysis on aggregated data ---")

# Refine global_patient_progression_details: add 'chronological_diagnoses'
patients_to_remove = []
for patient_id, data in global_patient_progression_details.items():
    if 'diagnoses' in data and data['diagnoses']:
        # Ensure 'diagnoses' is sorted by stage number
        sorted_diagnoses_by_stage = dict(sorted(data['diagnoses'].items()))
        global_patient_progression_details[patient_id]['diagnoses'] = sorted_diagnoses_by_stage

        # Create 'chronological_diagnoses' sorted by date
        global_patient_progression_details[patient_id]['chronological_diagnoses'] = dict(
            sorted(sorted_diagnoses_by_stage.items(), key=lambda item: item[1])
        )
    else: # Mark patient for removal if no valid diagnoses were aggregated
        patients_to_remove.append(patient_id)

# Remove patients marked for removal
for patient_id in patients_to_remove:
    del global_patient_progression_details[patient_id]


# Calculate transition times using final aggregated data
global_progression_times = {key: [] for key in stage_transitions.keys()}
patient_transition_output_list = []

for patient_id, data in global_patient_progression_details.items():
    patient_earliest_stage_dates = data.get('diagnoses', {}) # Use 'diagnoses' which has earliest dates

    patient_record = {
        'patient_id': patient_id,
        # Display chronological stages based on 'chronological_diagnoses'
        'stage_diagnoses_dates': [{'stage': s, 'date': d.strftime('%Y-%m-%d')} for s, d in data.get('chronological_diagnoses', {}).items()],
        'calculated_transitions': []
    }

    for transition_name, (from_stage, to_stage) in stage_transitions.items():
        if from_stage in patient_earliest_stage_dates and to_stage in patient_earliest_stage_dates:
            date_from = patient_earliest_stage_dates[from_stage]
            date_to = patient_earliest_stage_dates[to_stage]

            if date_to > date_from: # Ensure progression is forward in time
                duration_days = (date_to - date_from).days
                global_progression_times[transition_name].append(duration_days)
                patient_record['calculated_transitions'].append(
                    f"{transition_name}: {duration_days} days (From {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')})"
                )

    if patient_record['stage_diagnoses_dates']: # Only add if there's some stage info for the patient
        patient_transition_output_list.append(patient_record)


# Compute mean, median, and mode durations
summary_statistics = []
for transition_name, durations in global_progression_times.items():
    if durations:
        mean_duration = np.mean(durations)
        median_duration = np.median(durations)

        # Calculate Mode
        data_counts = collections.Counter(durations)
        # handle case where there might not be a single mode (all values unique)
        if data_counts:
             max_freq = max(data_counts.values())
             # Get all modes (there might be ties)
             modes = [value for value, freq in data_counts.items() if freq == max_freq]
             # Decide how to display modes - listing all or just one is common.
             # We'll list all modes if there are ties.
             mode_duration_info = ", ".join(map(str, modes)) # Join multiple modes with comma
             mode_frequency = max_freq if modes else 0
        else:
            mode_duration_info = "N/A (no data)"
            mode_frequency = 0


        count = len(durations)
    else:
        mean_duration = np.nan
        median_duration = np.nan
        mode_duration_info = "N/A (no data)"
        mode_frequency = 0
        count = 0
    summary_statistics.append({
        'Transition': transition_name,
        'Mean Duration (days)': round(mean_duration, 2) if not np.isnan(mean_duration) else 'N/A',
        'Median Duration (days)': round(median_duration, 2) if not np.isnan(median_duration) else 'N/A',
        'Mode Duration(s) (days)': mode_duration_info,
        'Mode Frequency': mode_frequency,
        'Number of Transitions Observed': count
    })
summary_df = pd.DataFrame(summary_statistics)

# --- Output Final Findings ---
print("\n--- Overall CKD Stage Progression Time Summary (from all archives) ---")
if not summary_df.empty:
    # Sort the summary table by the order of transitions defined
    transition_order = list(stage_transitions.keys())
    # Use .get(transition_name, len(transition_order)) to handle cases where a transition might not appear
    summary_df['Transition_Order'] = summary_df['Transition'].apply(lambda x: transition_order.index(x) if x in transition_order else len(transition_order))
    summary_df = summary_df.sort_values('Transition_Order').drop('Transition_Order', axis=1)
    print(summary_df.to_string(index=False))
else:
    print("No progression data to summarize from any of the archives.")

print(f"\nTotal unique patients with any CKD-related SNOMED code (1997-2023): {len(all_ckd_patient_ids_overall_set)}")
print(f"Total patients considered in progression analysis (had at least one defined CKD stage 1-6): {len(global_patient_progression_details)}")


print("\n--- Patient-Specific CKD Stage Transitions (Sample from all archives) ---")
if not patient_transition_output_list:
    print("No patient transition data to display.")
else:
    displayed_count = 0
    # Sort patients by ID for consistent sampling
    patient_transition_output_list.sort(key=lambda x: x['patient_id'])
    for record in patient_transition_output_list:
        if displayed_count < 10: # Displaying for first 10 patients from the aggregated list
            print(f"\nPatient ID: {record['patient_id']}")
            print("  Diagnosed Stages (Globally Earliest Dates, Chronological):")
            if record['stage_diagnoses_dates']:
                 for diag in record['stage_diagnoses_dates']:
                    print(f"    Stage {diag['stage']} on {diag['date']}")
            else:
                 print("    No specific stages (1-6) found.")

            if record['calculated_transitions']:
                print("  Calculated Progression Durations:")
                for trans_info in record['calculated_transitions']:
                    print(f"    {trans_info}")
            else:
                print("  No sequential stage progressions calculated along defined paths for this patient.")
            displayed_count += 1
        else:
            break # Stop displaying after the sample size
    if len(patient_transition_output_list) > displayed_count:
        print(f"\n... and {len(patient_transition_output_list) - displayed_count} more patients with CKD stage data.")

print("\n--- Analysis of all archives complete. ---")
