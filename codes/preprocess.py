import pandas as pd
import numpy as np
from pathlib import Path
import datetime

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

def getTimeStamps(chart_lab_events, con):
	min_time = chart_lab_events['charttime'].min()
	max_time = chart_lab_events['charttime'].max()
	min_time = datetime.datetime(min_time.year, min_time.month, min_time.day, min_time.hour)
	max_time = datetime.datetime(max_time.year, max_time.month, max_time.day, max_time.hour if max_time.minute == 0 else max_time.hour+1)
	hour = datetime.timedelta(hours=1)
	gap = max_time - min_time
	gap_in_hours = gap.days*24+gap.seconds//3600
	return pd.Series([min_time+hour*i for i in range(gap_in_hours+1)])

def populateColumn(column, chart_lab_events, itemids, time_series):
	values = chart_lab_events[chart_lab_events['itemid'].isin(itemids)]
	values = values.sort_values('charttime')
	def getValueForTimestamp(row):
		time = row['Time']
		# get value within one hour
		half_hour = datetime.timedelta(minutes=30)
		values_within_one_hour = values[(values['charttime']-time <= half_hour) & (time-values['charttime'] <= half_hour)]
		if len(values_within_one_hour):
			return values_within_one_hour['valuenum'].mean()
		# fill nan later
		return None
	i = 0
	half_hour = datetime.timedelta(minutes=30)
	column_values = []
	for time in time_series['Time']:
		values_within_one_hour = []
		while i < len(values):
			time_of_value = values.iloc[i]['charttime']
			if time_of_value >= time + half_hour:
				break
			if time_of_value >= time - half_hour:
				values_within_one_hour.append(values.iloc[i]['valuenum'])
			i += 1
		if values_within_one_hour:
			column_values.append(sum(values_within_one_hour)/len(values_within_one_hour))
		else:
			# fill na later
			column_values.append(None)
	time_series[column] = pd.Series(column_values)
	# fill na
	time_series[column].fillna(method='ffill', inplace=True)
	time_series[column].fillna(method='backfill', inplace=True)

def getICUStayTimeSeries(patient_stay, con):
	icustay_id = patient_stay['icustay_id']
	hadm_id = patient_stay['hadm_id']
	chart_lab_events, procedures = getAllEvents(hadm_id, con)
	chart_lab_events = chart_lab_events[(chart_lab_events['charttime'] >= patient_stay['intime']) & (chart_lab_events['charttime'] <= patient_stay['outtime'])]
	chart_lab_events['charttime_nearest_hour'] = chart_lab_events['charttime'].apply(lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour if dt.minute <= 30 else dt.hour+1))
	time_series = pd.DataFrame({'Time': getTimeStamps(chart_lab_events, con)})
	populateColumn('SpO2', chart_lab_events, [646, 834, 220277], time_series)
	populateColumn('Temperature', chart_lab_events, [50825, 676, 677], time_series)
	populateColumn('Heart Rate', chart_lab_events, [211, 220045], time_series)
	populateColumn('CVP', chart_lab_events, [113, 220074], time_series)
	populateColumn('Hematocrit', chart_lab_events, [51221], time_series)
	populateColumn('Potassium', chart_lab_events, [50971], time_series)
	populateColumn('Sodium', chart_lab_events, [50983], time_series)
	populateColumn('Creatinine', chart_lab_events, [50912], time_series)
	populateColumn('Chloride', chart_lab_events, [50902], time_series)
	populateColumn('Urea Nitrogen', chart_lab_events, [51006], time_series)
	populateColumn('Platelet Count', chart_lab_events, [51265], time_series)
	populateColumn('White Blood Cells', chart_lab_events, [51301], time_series)
	populateColumn('Red Blood Cells', chart_lab_events, [51279], time_series)
	populateColumn('Calculated Total CO2', chart_lab_events, [50804], time_series)
	populateColumn('pH', chart_lab_events, [50820], time_series)
	return time_series
