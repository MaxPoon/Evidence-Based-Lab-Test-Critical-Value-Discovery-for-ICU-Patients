import pandas as pd
import numpy as np

def getICUStayPatients(con):
	icustays = pd.read_sql_query("""
		SELECT i.icustay_id, p.gender, p.dob, i.intime, p.dod, i.los
		FROM icustays AS i
		LEFT JOIN patients AS p ON i.subject_id=p.subject_id
		WHERE first_wardid=last_wardid
		AND first_careunit=last_careunit
		AND los>=1 AND i.hadm_id IN
			(SELECT hadm_id FROM icustays GROUP BY hadm_id HAVING COUNT(1)=1);
		""", con=con)
	icustays['age'] = icustays['intime'] - icustays['dob']
	icustays['age'] = (icustays['age'] / np.timedelta64(1, 'Y')).astype(int)
	icustays = icustays[icustays['age'] >= 18]
	icustays.reset_index(inplace=True, drop=True)
	return icustays

def getICUStayTimeSeries(icustay_id, con):
	pass
