import pandas as pd
import numpy as np
from pathlib import Path

itemidToMeasurement = {
	211: 'Heart Rate',
	646: 'SpO2',
	834: 'SaO2',
	220277: 'O2 saturation pulseoxymetry',
	618: 'Respiratory Rate',
	220210: 'Respiratory Rate',
	220045: 'Heart Rate',
	220074: 'Central Venous Pressure',
	113: 'CVP',
	677: 'Temperature C (calc)',
	676: 'Temperature C'
}

itemidToLabEvent = {
	51221: 'Hematocrit',
	50971: 'Potassium',
	50983: 'Sodium',
	50912: 'Creatinine',
	50902: 'Chloride',
	51006: 'Urea Nitrogen',
	51265: 'Platelet Count',
	51301: 'White Blood Cells',
	51279: 'Red Blood Cells',
	50804: 'Calculated Total CO2',
	50820: 'pH',
	50825: 'Temperature'
}

def getICUStayPatients(con, force_reload=False):
	icustays_filepath = './selected_stays.csv'
	icustays_file = Path(icustays_filepath)
	if not force_reload and icustays_file.is_file():
		icustays = pd.read_csv(icustays_filepath)
		return icustays
	icustays = pd.read_sql_query("""
		SELECT i.hadm_id, i.icustay_id, p.gender, p.dob, i.intime, i.outtime, p.dod, i.los
		FROM icustays AS i
		LEFT JOIN patients AS p ON i.subject_id=p.subject_id
		WHERE first_wardid=last_wardid
		AND first_careunit=last_careunit
		AND los>=1;
		""", con=con)
	# calculate the age
	icustays['age'] = icustays['intime'] - icustays['dob']
	icustays['age'] = (icustays['age'] / np.timedelta64(1, 'Y')).astype(int)
	icustays = icustays[icustays['age'] >= 16]
	# create stay count filter
	stay_count_filter = pd.read_sql_query('SELECT hadm_id FROM icustays GROUP BY hadm_id HAVING COUNT(1)=1;', con=con)
	# create chartevent filters
	oxygen_filter = pd.read_sql_query('SELECT DISTINCT hadm_id FROM chartevents WHERE itemid=646 OR itemid=834 OR itemid=220277 AND valuenum IS NOT NULL;', con=con)
	heartrate_filter = pd.read_sql_query('SELECT DISTINCT hadm_id FROM chartevents WHERE itemid=211 OR itemid=220045 AND valuenum IS NOT NULL;', con=con)
	cvp_filter = pd.read_sql_query('SELECT DISTINCT hadm_id FROM chartevents WHERE itemid=113 OR itemid=220074 AND valuenum IS NOT NULL;', con=con)
	respiratory_filter = pd.read_sql_query('SELECT DISTINCT hadm_id FROM chartevents WHERE itemid=618 OR itemid=220210 AND valuenum IS NOT NULL;', con=con)
	# create labevent filters
	selected_labitems = [51221, 50971, 50983, 50912, 50902, 51006, 51265, 51265, 51301, 51279, 50804, 50820]
	lab_filters = [pd.read_sql_query('SELECT DISTINCT hadm_id FROM labevents WHERE itemid={itemid} AND valuenum IS NOT NULL;'.format(itemid=itemid), con=con) for itemid in selected_labitems]
	# create temperature event filters
	temp_filter = pd.read_sql_query("""
		SELECT DISTINCT hadm_id FROM chartevents WHERE itemid=676 OR itemid=677 AND valuenum IS NOT NULL
		UNION
		SELECT DISTINCT hadm_id FROM labevents WHERE itemid=50825 AND valuenum IS NOT NULL;
		""", con=con)
	filters = [stay_count_filter, oxygen_filter, heartrate_filter, cvp_filter, respiratory_filter, temp_filter] + lab_filters
	# filter the icustays with filters
	for feature_filter in filters:
		icustays = icustays[icustays['hadm_id'].isin(feature_filter['hadm_id'])]
	# convert gender to binary values
	icustays['gender'] = (icustays['gender'] == 'M').astype(int)
	icustays.reset_index(inplace=True, drop=True)
	icustays.to_csv(icustays_filepath, index=False)
	return icustays

def getAllEvents(hadm_id, con):
	labevent_ids = [51221, 50971, 50983, 50912, 50902, 51006, 51265, 51265, 51301, 51279, 50804, 50825, 50820]
	labevent_ids_str = '(' + ','.join(list(map(str, labevent_ids))) +')'
	chartevent_ids = [211, 646, 834, 618, 220277, 220210, 220045, 220074, 113, 677, 678]
	chartevent_ids_str = '(' + ','.join(list(map(str, chartevent_ids))) + ')'
	chart_lab_events = pd.read_sql_query("""
		SELECT itemid, charttime, valuenum FROM chartevents
		WHERE hadm_id={hadm_id}
		AND itemid IN {chartevent_ids_str}
		AND valuenum IS NOT NULL
		UNION ALL
		SELECT itemid, charttime, valuenum FROM labevents
		WHERE hadm_id={hadm_id}
		AND itemid IN {labevent_ids_str}
		AND valuenum IS NOT NULL;
		""".format(hadm_id=hadm_id, chartevent_ids_str=chartevent_ids_str, labevent_ids_str=labevent_ids_str), con=con)
	procedures = pd.read_sql_query("""
		SELECT starttime, endtime, itemid FROM procedureevents_mv
		WHERE hadm_id={hadm_id}
		AND (itemid=225792 OR itemid=225794);
		""".format(hadm_id=hadm_id), con=con)
	return chart_lab_events, procedures

def getICUStayTimeSeries(patient_stay, con):
	icustay_id = patient_stay['icustay_id']
	hadm_id = patient_stay['hadm_id']
	chart_lab_events, procedures = getAllEvents(hadm_id, con)
	
