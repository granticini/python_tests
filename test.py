import logging
import csv

def DBG(msg):
	logging.debug(msg)
	print(msg)

DBG("START")

def find_column_index(columns, name):
	idx = -1
    
	try:
		idx = columns.index(name.lower())
	except ValueError:
		print( "Could not find ", name)
    
	return idx
    
with open('input.csv', 'r') as csvfile:
	csvReader = csv.reader(
		csvfile,
		delimiter=",",
		quotechar='"',
		quoting=csv.QUOTE_MINIMAL,
		skipinitialspace=True)

	rowNumber = 0
	for row in csvReader:
		rowNumber = rowNumber + 1
		if (rowNumber == 1):
			DBG("First record")
			header = row
			DBG(header)

			for idx in range(len(header)):
				header[idx] = header[idx].lower()
			DBG(header)

			DBG("Header contains " + str(len(header)) + " elements")

			nameIndex = find_column_index(header, "name")
			emailIndex = find_column_index(header, "Email")
			otherIndex = find_column_index(header, "other")

			DBG("nameIndex =" + str(nameIndex))
			DBG("emailIndex=" + str(emailIndex))
			DBG("otherIndex=" + str(otherIndex))

			if nameIndex < 0:
				print("Could not find Name field")

			if emailIndex < 0:
				print("Could not find Email field")
		else:
			DBG("Row# " + str(rowNumber))
			name = row[nameIndex].strip()
			email = row[emailIndex].strip()
			DBG(name + ", " + email)
			#DBG(row)
		
		if rowNumber >= 100:
			break

DBG("----------")
    
with open('input.csv', 'r') as csvfile:
	csvReader = csv.DictReader(
		csvfile,
		delimiter=",",
		quotechar='"',
		quoting=csv.QUOTE_MINIMAL,
		skipinitialspace=True)

	rowNumber = 0
	for row in csvReader:
		rowNumber = rowNumber + 1

		DBG("Row# " + str(rowNumber))
		name = row["name"].strip()
		email = row["eMail"].strip()
		other = row["Other"].strip()
		if (other is None):
			other = ""
		DBG(", ".join([name, email, other]))
		
		if rowNumber >= 100:
			break

DBG("*** END ***")
