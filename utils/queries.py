from sqlalchemy import create_engine
import pandas as pd

engine = None

def getEngine():
	global engine
	if engine:
		return engine
	try:
		# TODO: get credentials from local config file
		engine = create_engine('postgresql://maxpoonr@localhost:5432/mimic')
		return engine
	except:
		print("Unable to connect to the database")
		return None

def getPatients():
	engine = getEngine()
	patients = pd.read_sql_query('SELECT * FROM patients;',con=engine)
	return patients

def getPatientsOfGender(gender):
	engine = getEngine()
	patients = pd.read_sql_query('SELECT * FROM patients WHERE gender = "{gender}";'.format(gender=gender),con=engine)
	return patients

def selectColumnsFromTable(columns, table):
	engine = getEngine()
	columnsStr = ' ,'.join(columns)
	results = pd.read_sql_query('SELECT {columns} FROM {table};'.format(columns=columnsStr, table=table),con=engine)
	return results

def selectColumnsFromTableWithConditions(columns, table, conditions):
	engine = getEngine()
	columnsStr = ' ,'.join(columns)
	conditionsStr = ' ,'.join(conditions)
	results = pd.read_sql_query('SELECT {columns} FROM {table} WHERE {conditions};'.format(columns=columnsStr, table=table, conditions=conditionsStr),con=engine)
	return results
