# CKD Stage Progression Analysis

This repository provides a Python script for analyzing Chronic Kidney Disease (CKD) stage progression using SNOMED CT codes from patient condition data. The script identifies CKD diagnoses, maps them to specific CKD stages, and calculates stage progression durations for patients over a defined time period (1997-2023).

## Dataset Requirements

The script expects the following CSV files in the working directory:

* `patients.csv`: Contains patient demographic data (ID, BIRTHDATE, DEATHDATE, FIRST, LAST, GENDER).
* `conditions.csv`: Contains patient condition data (START, STOP, PATIENT, ENCOUNTER, CODE, DESCRIPTION).

### Sample `patients.csv` Structure:

| Id  | BIRTHDATE  | DEATHDATE  | FIRST | LAST | GENDER |
| --- | ---------- | ---------- | ----- | ---- | ------ |
| 001 | 1960-05-10 | 2023-07-10 | John  | Doe  | M      |

### Sample `conditions.csv` Structure:

| START      | STOP       | PATIENT | ENCOUNTER | CODE      | DESCRIPTION |
| ---------- | ---------- | ------- | --------- | --------- | ----------- |
| 1997-01-05 | 1997-02-10 | 001     | 123       | 431855005 | CKD Stage 1 |

## How to Run

1. Ensure that the required datasets (`patients.csv` and `conditions.csv`) are present in the working directory.
2. Run the notebook:

```bash
jupyter notebook Internship_Eval_25.ipynb
```

## Script Overview

* **Data Preparation:** Loads patient and condition data, handles missing files, and filters data for the specified date range.
* **CKD Identification:** Identifies patients with CKD diagnoses using relevant SNOMED CT codes and maps them to stages (1-6).
* **Stage Tracking:** Tracks each patient's stage progression and calculates time periods between stage transitions.
* **Statistical Analysis:** Calculates mean and median durations for each CKD stage transition.
* **Output:** Displays summary statistics and patient-specific transition details.

## Key SNOMED CT Codes for CKD:

* 431855005: CKD Stage 1
* 431856006: CKD Stage 2
* 433144002: CKD Stage 3
* 431857002: CKD Stage 4
* 433146000: CKD Stage 5
* 714152005: CKD Stage 5 on dialysis (End Stage Renal Disease - ESRD)

## Output Structure

* **Stage Progression Summary:** Displays mean, median, and count of patients for each stage transition.
* **Patient-Specific Transitions:** Displays each patient's diagnosed stages and progression durations.

## Example Output:

```
--- CKD Stage Progression Time Summary ---

  Transition                   Mean Duration (days)   Median Duration (days)   Number of Patients
  Stage 1 to Stage 2           180                     150                      12
  Stage 2 to Stage 3           220                     210                      8

--- Patient-Specific CKD Stage Transitions ---

Patient ID: 001
  Diagnosed Stages (Earliest Dates):
    Stage 1 on 1997-01-05
    Stage 2 on 1997-08-15
  Calculated Progression Durations:
    Stage 1 to Stage 2: 222 days (From 1997-01-05 to 1997-08-15)
```

## Implementation Notes

* The script handles missing files by using dummy data to maintain code execution.
* Multiple diagnoses on the same day are resolved by selecting the highest stage for that day.
* Stage 0 is used to identify general CKD presence but is excluded from stage-to-stage progression calculations.
* ESRD (Stage 6) is determined from dialysis-related diagnoses.

This project is licensed under the MIT License. See the `LICENSE` file for more information.
