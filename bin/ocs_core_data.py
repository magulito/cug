import xlsxwriter, sqlite3, os, sys, re, csv, time, shutil, datetime, logging, locale
import threading, multiprocessing, openpyxl
import pandas as pd
from cug_config_file import *

def DataBase_Connection():
	
	'''
	
	This module will create the databse in the memory, create tables and
	call the functions that will import data from OCS output files.

	'''

	try:

		global connectionDB
		global cursor
		
		connectionDB = sqlite3.connect(':memory:')
		cursor = connectionDB.cursor()
		Create_tables()
	
	except Exception as e:

		print(e)

def Create_tables():

	try:

		cursor = connectionDB.cursor()

		cursor.execute('DROP TABLE IF EXISTS tblVxView_Subscribers')
		connectionDB.commit()

		cursor.execute('DROP TABLE IF EXISTS tblOCS_Subscribers')
		connectionDB.commit()

		cursor.execute('DROP TABLE IF EXISTS tblOCS_UserGroupMembers')
		connectionDB.commit()

		cursor.execute('DROP TABLE IF EXISTS tblOCS_CUGPackage')
		connectionDB.commit()

		cursor.execute('DROP TABLE IF EXISTS tblOCS_CUGPackageGroupID')
		connectionDB.commit()

		cursor.execute('CREATE TABLE IF NOT EXISTS tblVxView_Subscribers(VxView_Subscriber TEXT, VxView_GroupID TEXT)')
		connectionDB.commit()

		cursor.execute('CREATE TABLE IF NOT EXISTS tblOCS_Subscribers(OCS_Subscriber TEXT, OCS_OfferID TEXT)')
		connectionDB.commit()

		cursor.execute('CREATE TABLE IF NOT EXISTS tblOCS_UserGroupMembers(OCS_Subscriber TEXT, OCS_GroupID TEXT)')
		connectionDB.commit()

		cursor.execute('CREATE TABLE IF NOT EXISTS tblOCS_CUGPackage(OCS_Subscriber TEXT, CUG_Package TEXT)')
		connectionDB.commit()

		cursor.execute('CREATE TABLE IF NOT EXISTS tblOCS_CUGPackageGroupID(OCS_Subscriber TEXT, CUG_Package TEXT, OCS_GroupID TEXT)')
		connectionDB.commit()

		for batch_command_file in os.listdir(batch_commands_path):

			os.remove(os.path.join(batch_commands_path, batch_command_file))

	except Exception as e:

		print(e)

def cug_command_ReadOfferID(VxView_Subscriber):

	try:

		global batch_command

		batch_command = 'GMF::EntireRead:Customer(CustomerId="' + VxView_Subscriber + '", @SelectionDate=NOW) (ROP{CustomerId,s_OfferId});'

		create_batch_commands(VxView_Subscriber)
	
	except Exception as e:

		print(e)

def cug_command_RetrieveReadUserGroupMember(VxView_Subscriber):

	try:

		global batch_command

		batch_command = 'CUG::RetrieveRead:UserGroupMembers(MSISDN); ("00' + VxView_Subscriber + '");'

		create_batch_commands(VxView_Subscriber)

	except Exception as e:
		
		print(e)

def cug_command_EntireReadPackages(VxView_Subscriber):

	try:

		global batch_command

		batch_command = 'GMF::EntireRead:Customer(CustomerId="' + VxView_Subscriber + '",@SelectionDate=NOW) (RPP{CustomerId,s_PackageId,s_CUGNumbers});'

		create_batch_commands(VxView_Subscriber)

	except Exception as e:

		print(e)

def cug_command_UserGroupMembers(VxView_Subscriber, VxView_GroupID):

	try:

		global batch_command

		batch_command = 'CUG::Read:UserGroupMembers(GroupId=' + VxView_GroupID + ',MSISDN = "00' + VxView_Subscriber + '");'

		create_batch_commands(VxView_Subscriber)

	except Exception as e:
		
		print(e)

def Read_VxView_Dump():
	
	try:

		global VxView_Subscriber
		global VxView_GroupID

		for i in os.listdir(batch_commands_path):

			os.remove(os.path.join(batch_commands_path, i))

		record_counter = 0
		
		if (os.path.isfile(VxView_CUG_Dump)):
			
			print("Please, wait while VxView Subscribers are being imported to the local Database.")

			with open(VxView_CUG_Dump) as cug_alertfile:  
				for cnt, line_cug_alert in enumerate(cug_alertfile):
	
					if not line_cug_alert.startswith("U"):

						record_counter = record_counter + 1
	
						VxView_Subscriber = line_cug_alert.split(',')[3]
						VxView_GroupID = line_cug_alert.split(',')[1]

						Insert_VxView_Subscriber(VxView_Subscriber, VxView_GroupID)

						if not os.listdir(output_xml):

							cug_command_ReadOfferID(VxView_Subscriber)
							cug_command_EntireReadPackages(VxView_Subscriber)
							cug_command_RetrieveReadUserGroupMember(VxView_Subscriber)
							cug_command_UserGroupMembers(VxView_Subscriber, VxView_GroupID)
		
		print("Total VxView Subscribers:", record_counter)

	except Exception as e:
		
		print(e)

def create_batch_commands(VxView_Subscriber):

	try:

		if (VxView_Subscriber[5])	< '5':

			if not (os.path.isfile(txt_OCS1_ReadCUG_Config)):
								
				ReadCUG_Configuration_commands  = open(txt_OCS1_ReadCUG_Config ,"w+")

			ReadCUG_Configuration_commands = open(txt_OCS1_ReadCUG_Config ,"a")
			
		if (VxView_Subscriber[5])	> '4':

			if not (os.path.isfile(txt_OCS3_ReadCUG_Config)):

				ReadCUG_Configuration_commands  = open(txt_OCS3_ReadCUG_Config ,"w+")
		
			ReadCUG_Configuration_commands  = open(txt_OCS3_ReadCUG_Config ,"a")
	
		ReadCUG_Configuration_commands.write(batch_command + "\n")

		print(batch_command)

	except Exception as e:

		print(e)

def Insert_VxView_Subscriber(VxView_Subscriber, VxView_GroupID):

	try:
		
		cursor.execute('INSERT INTO tblVxView_Subscribers VALUES (?,?)', (VxView_Subscriber, VxView_GroupID))   
		connectionDB.commit()

	except Exception as e:

		print(e)

def Insert_OCS_Subscriber(OCS_Subscriber, OCS_OfferID):
	
	try:

		cursor.execute('INSERT INTO tblOCS_Subscribers VALUES (?,?)', (OCS_Subscriber, OCS_OfferID))
		connectionDB.commit()

		print('OCS Subscriber:', OCS_Subscriber, 'OfferID:', OCS_OfferID, "has been saved to OCS DataBase")

	except Exception as e:

		print(e)

def Insert_OCS_UserGroupMembers(OCS_Subscriber, OCS_GroupID):

	try:
	
		cursor.execute('INSERT INTO tblOCS_UserGroupMembers VALUES (?,?)', (OCS_Subscriber, OCS_GroupID))
		connectionDB.commit()

		print('OCS Subscriber:', OCS_Subscriber, 'OCS_GroupID:', OCS_GroupID, "has been saved to OCS DataBase")

	except Exception as e:

		print(e)

def Insert_OCS_CUG_Package(OCS_Subscriber, CUG_Package):

	try:
	
		cursor.execute('INSERT INTO tblOCS_CUGPackage VALUES (?,?)', (OCS_Subscriber, CUG_Package))
		connectionDB.commit()

		print('OCS Subscriber:', OCS_Subscriber, 'CUG_Package:', CUG_Package, "has been saved to OCS DataBase")
	
	except Exception as e:

		print(e)

def Insert_OCS_CUGPackageGroupID(OCS_Subscriber, CUG_Package, OCS_Package_GroupID):

	try:

		cursor.execute('INSERT INTO tblOCS_CUGPackageGroupID VALUES (?,?,?)', (OCS_Subscriber, CUG_Package, OCS_Package_GroupID))
		connectionDB.commit()
		
		print('OCS Subscriber:', OCS_Subscriber, 'CUG_Package', CUG_Package, 'OCS_GroupID:', OCS_Package_GroupID, "has been saved to OCS DataBase")

	except Exception as e:

		print(e)

def OCS_outputfile_RetrieveOfferID():
	
	try:

		for OCS_OfferID_file in os.listdir(output_xml):
			if OCS_OfferID_file in ['OCS1_ReadCUG_OfferID.out', 'OCS3_ReadCUG_OfferID.out']:

				OCS_subscribers_count = 0
				list_OCS_OfferID = []

				with open(output_xml + '\\' + OCS_OfferID_file) as Read_OfferID_OutputFile:  
					for cnt, line in enumerate(Read_OfferID_OutputFile):
								
						if line.startswith('GMF::Read:ROP'):# Extracts the MSISDn and OfferID
	
							OCS_subscribers_count = OCS_subscribers_count + 1
							OCS_Subscriber = (re.findall("\d+", line))[0]	
							OCS_OfferID = (re.findall("\d+", line))[1]
	
							Insert_OCS_Subscriber(OCS_Subscriber, OCS_OfferID)#save data to database

	except Exception as e:

		print(e)

def OCS_outputfile_Retrieve_UserGroupMembershiptable():

	try:

		for UserGroupMembership in os.listdir(output_xml):
			if UserGroupMembership in ['OCS1_ReadCUG_UserGroupIDMembers.out', 'OCS3_ReadCUG_UserGroupIDMembers.out']:
			
				list_UserGroupMembership_counter = 0
	
				with open(output_xml + '\\' + UserGroupMembership) as UserGroupMembershipTable: # For reading the UserGroupID on UserGroupMembers Table. 
					for cnt, line_usergroup_member in enumerate(UserGroupMembershipTable):
					
						if line_usergroup_member.startswith('("1","'):
	
							list_UserGroupMembership_counter = list_UserGroupMembership_counter + 1
	
							OCS_Subscriber = re.findall("\d+", line_usergroup_member)[1]
							OCS_GroupID = re.findall("\d+", line_usergroup_member)[2]
	
							Insert_OCS_UserGroupMembers(OCS_Subscriber, OCS_GroupID)

	except Exception as e:
		print(e)

def OCS_outputfile_Retrieve_CUGPackage():

	try:

		for CUGPackage in os.listdir(output_xml):
			if CUGPackage in ['OCS1_ReadCUG_Package.out', 'OCS3_ReadCUG_Package.out']:
		
				with open(output_xml + '\\' + CUGPackage) as OCSReadCUGPKGFile:  
					for cnt, line_cug in enumerate(OCSReadCUGPKGFile):
					
						if line_cug.startswith('GMF::Read:RPP'): #Filter packages
	
							OCS_Subscriber = re.findall("\d+", line_cug)[0]
							CUG_Package = (line_cug.split(',')[1].split('=')[1].split('"')[1])
	
							if CUG_Package in ("CUG", "CUG_500", "mPesa_Support"):
	
								CUG_Package = (line_cug.split(',')[1].split('=')[1].split('"')[1])
	
								Insert_OCS_CUG_Package(OCS_Subscriber, CUG_Package)
			
	except Exception as e:
		print(e)

def OCS_outputfile_Retrieve_CUGPackageGroupID():

	try:

		for CUGPackage in os.listdir(output_xml):

			if CUGPackage in ['OCS1_ReadCUG_PackageGroupID.out', 'OCS3_ReadCUG_PackageGroupID.out']:
			
				OCS_Package_GroupID = ''
	
				with open(output_xml + '\\' + CUGPackage) as OCSReadCUGPKGFile:  
					for cnt, line_cug in enumerate(OCSReadCUGPKGFile):
					
						if line_cug.startswith('GMF::Read:RPP'): #Filter packages
	
							OCS_Subscriber = re.findall("\d+", line_cug)[0]
							OCS_Package_GroupID = 'NA'
							CUG_Package = 'NA'
	
							CUG_Package = (line_cug.split(',')[1].split('=')[1].split('"')[1])

							if CUG_Package in ("CUG", "CUG_500", 'mPesa_Support'):
	
								OCS_Subscriber = re.findall("\d+", line_cug)[0]

								CUG_Package_list = line_cug.split(",")

								if len(CUG_Package_list) > 2:

									CUG_Package_list = re.findall("\d+",((line_cug.split("=")[-1])))

									for i in CUG_Package_list:

										OCS_Package_GroupID = i

										Insert_OCS_CUGPackageGroupID(OCS_Subscriber, CUG_Package, OCS_Package_GroupID)
								else:

									CUG_Package_list = 'NA'
												
	except Exception as e:
		print(e)

def readDB_VxView_Subscribers():
	
	try:

		cursor.execute('SELECT VxView_Subscriber, VxView_GroupID FROM tblVxView_Subscribers ORDER BY VxView_Subscriber, VxView_GroupID')   
		connectionDB.commit()

		counter = 0

		for i in cursor.fetchall():

			counter = counter + 1
			print(i[0], i[1])

		print("Total VxView Subscribers:", counter)
		print("----------------------------------")

	except Exception as e:

		print(e)

def readDB_OCS_Subscribers():
	
	try:

		cursor.execute('SELECT OCS_Subscriber, OCS_OfferID FROM tblOCS_Subscribers ORDER BY OCS_Subscriber, OCS_OfferID')   
		connectionDB.commit()

		counter = 0

		for i in cursor.fetchall():

			counter = counter + 1
			print(i[0], i[1])

		print("Total OCS Subscribers:", counter)
		print("-------------------------------")
	
	except Exception as e:

		print(e)

def readDB_OCS_UserGroupMembers():
	
	try:

		cursor.execute('SELECT OCS_Subscriber, OCS_GroupID FROM tblOCS_UserGroupMembers ORDER BY OCS_Subscriber, OCS_GroupID')   
		connectionDB.commit()

		counter = 0

		for i in cursor.fetchall():

			counter = counter + 1
			print(i[0], i[1])

		print("Total records on tblOCS_UserGroupMembers:", counter)
		print("--------------------------------------------------")
	
	except Exception as e:

		print(e)

def readDB_OCS_CUGPackage():
	
	try:

		cursor.execute('SELECT OCS_Subscriber, CUG_Package FROM tblOCS_CUGPackage ORDER BY OCS_Subscriber, CUG_Package')   
		connectionDB.commit()

		counter = 0

		for i in cursor.fetchall():

			counter = counter + 1
			print(i[0], i[1])
		
		print("Total OCS Subscribers with CUG Package:", counter)
		print("------------------------------------------------")
	
	except Exception as e:

		print(e)

def readDB_OCS_CUGPackageGroupID():
	
	try:

		cursor.execute('SELECT OCS_Subscriber, CUG_Package, OCS_GroupID FROM tblOCS_CUGPackageGroupID WHERE OCS_Subscriber ORDER BY OCS_Subscriber, CUG_Package, OCS_GroupID')   
		connectionDB.commit()

		counter = 0
		
		for i in cursor.fetchall():

			counter = counter + 1
			print(i[0], i[1], i[2])
		
		print("Total OCS Subscribers with GroupID on CUG Package:", counter)
		print("-----------------------------------------------------------")
	
	except Exception as e:

		print(e)

def readDB_OCS_all_data():
	
	try:

		ocs_sqlquery = '''
					
					SELECT DISTINCT tblOCS_Subscribers.OCS_Subscriber AS OCS_Subscriber, OCS_OfferID, tblOCS_UserGroupMembers.OCS_GroupID AS UserGroupMembersGroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID AS CUGPACKAGE_GROUPID
					FROM tblOCS_Subscribers

					LEFT JOIN tblOCS_UserGroupMembers
					ON tblOCS_Subscribers.OCS_Subscriber = ltrim(tblOCS_UserGroupMembers.OCS_Subscriber, '00')

					LEFT JOIN tblOCS_CUGPackage
					ON tblOCS_Subscribers.OCS_Subscriber = tblOCS_CUGPackage.OCS_Subscriber

					LEFT JOIN tblOCS_CUGPackageGroupID
					ON tblOCS_Subscribers.OCS_Subscriber = tblOCS_CUGPackageGroupID.OCS_Subscriber

					GROUP BY tblOCS_Subscribers.OCS_Subscriber, OCS_OfferID, UserGroupMembersGroupID, tblOCS_CUGPackage.CUG_Package, CUGPACKAGE_GROUPID
					ORDER BY tblOCS_Subscribers.OCS_Subscriber, OCS_OfferID, UserGroupMembersGroupID, tblOCS_CUGPackage.CUG_Package, CUGPACKAGE_GROUPID
					
					'''  
		cursor.execute(ocs_sqlquery)
		connectionDB.commit()

		counter = 0

		for i in cursor.fetchall():

			counter = counter + 1
			print(i[0], i[1], i[2], i[3], i[4])
			#print(i)

		print("Total number of records:", counter)
	
	except Exception as e:

		print(e)

def readDB_compare_profiles():
	
	try:

		compare_sqlquery = '''
					
					SELECT DISTINCT VxView_Subscriber, VxView_GroupID, OCS_OfferID, tblOCS_UserGroupMembers.OCS_GroupID AS UserGroupMembersGroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID AS CUGPACKAGE_GROUPID
					FROM tblVxView_Subscribers
					
					LEFT JOIN tblOCS_Subscribers
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_Subscribers.OCS_Subscriber

					LEFT JOIN tblOCS_UserGroupMembers
					ON tblVxView_Subscribers.VxView_Subscriber = ltrim(tblOCS_UserGroupMembers.OCS_Subscriber, '00')

					LEFT JOIN tblOCS_CUGPackage
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_CUGPackage.OCS_Subscriber

					LEFT JOIN tblOCS_CUGPackageGroupID
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_CUGPackageGroupID.OCS_Subscriber

					GROUP BY VxView_Subscriber, VxView_GroupID, OCS_OfferID, tblOCS_UserGroupMembers.OCS_GroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID
					ORDER BY VxView_Subscriber, VxView_GroupID, OCS_OfferID, tblOCS_UserGroupMembers.OCS_GroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID
					
					'''  
		cursor.execute(compare_sqlquery)
		connectionDB.commit()

		counter = 0

		for i in cursor.fetchall():

			counter = counter + 1
			print(i[0], i[1], i[2], i[3], i[4], i[5])

		print("Total number of records:", counter)
	
	except Exception as e:

		print(e)

def readDB_Invalid_Subscribers():
	
	try:

		read_Invalid_Subscribers_sqlquery = '''
					
					SELECT DISTINCT VxView_Subscriber, VxView_GroupID, OCS_OfferID, tblOCS_UserGroupMembers.OCS_GroupID AS UserGroupMembersGroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID AS CUGPACKAGE_GROUPID
					FROM tblVxView_Subscribers
					
					LEFT JOIN tblOCS_Subscribers
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_Subscribers.OCS_Subscriber

					LEFT JOIN tblOCS_UserGroupMembers
					ON tblVxView_Subscribers.VxView_Subscriber = ltrim(tblOCS_UserGroupMembers.OCS_Subscriber, '00')

					LEFT JOIN tblOCS_CUGPackage
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_CUGPackage.OCS_Subscriber

					LEFT JOIN tblOCS_CUGPackageGroupID
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_CUGPackageGroupID.OCS_Subscriber

					WHERE OCS_OfferID NOT IN ('33', '34', '36', '37', '38', '39', '43', '44', '45', '46', '47', '48', '49', '55', '1000', '1001', '1002', '1003', '1004', '2000', '2001', '2002', '2003', '2004')

					GROUP BY VxView_Subscriber, VxView_GroupID, OCS_OfferID, tblOCS_UserGroupMembers.OCS_GroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID
					ORDER BY OCS_OfferID, VxView_Subscriber, VxView_GroupID,  tblOCS_UserGroupMembers.OCS_GroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID
					
					'''  
		cursor.execute(read_Invalid_Subscribers_sqlquery)
		connectionDB.commit()

		counter = 0

		for i in cursor.fetchall():

			counter = counter + 1

			VxView_Subscriber = i[0]

			cug_config_DeleteUserGroupMembersGroupAll(VxView_Subscriber)

			if i[4]:

				CUG_Package = i[4]
				cug_config_CUGPackage_Unsubscribe(VxView_Subscriber, CUG_Package)
			
			print(i[0], i[1], i[2], i[3], i[4], i[5])

		print("Total Invalid Subscribers on OCS:", counter)
		print("------------------------------------------")
	
	except Exception as e:

		print(e)

def readDB_Missing_Subscribers():
	
	try:

		read_Missing_Subscribers_sqlquery = '''
					
					SELECT DISTINCT VxView_Subscriber, VxView_GroupID, OCS_OfferID, tblOCS_UserGroupMembers.OCS_GroupID AS UserGroupMembersGroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID AS CUGPACKAGE_GROUPID
					FROM tblVxView_Subscribers
					
					LEFT JOIN tblOCS_Subscribers
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_Subscribers.OCS_Subscriber

					LEFT JOIN tblOCS_UserGroupMembers
					ON tblVxView_Subscribers.VxView_Subscriber = ltrim(tblOCS_UserGroupMembers.OCS_Subscriber, '00')

					LEFT JOIN tblOCS_CUGPackage
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_CUGPackage.OCS_Subscriber

					LEFT JOIN tblOCS_CUGPackageGroupID
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_CUGPackageGroupID.OCS_Subscriber

					WHERE VxView_Subscriber NOT IN (SELECT OCS_Subscriber FROM tblOCS_Subscribers)

					GROUP BY VxView_Subscriber, VxView_GroupID, OCS_OfferID, tblOCS_UserGroupMembers.OCS_GroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID
					ORDER BY VxView_Subscriber, VxView_GroupID, OCS_OfferID, tblOCS_UserGroupMembers.OCS_GroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID
					
					'''  
		cursor.execute(read_Missing_Subscribers_sqlquery)
		connectionDB.commit()

		counter = 0

		for i in cursor.fetchall():

			counter = counter + 1
			print(i[0], i[1], i[2], i[3], i[4], i[5])

			VxView_Subscriber = i[0]

			cug_config_DeleteUserGroupMembersGroupAll(VxView_Subscriber)

		print("Total missing Subscribers on OCS:", counter)
		print("------------------------------------------")
	
	except Exception as e:

		print(e)

def readDB_Missing_UserGroupMembers():
	
	try:

		read_Missing_Subscribers_sqlquery = '''
					
					SELECT DISTINCT VxView_Subscriber, VxView_GroupID, OCS_OfferID, tblOCS_UserGroupMembers.OCS_GroupID AS UserGroupMembersGroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID AS CUGPACKAGE_GROUPID
					FROM tblVxView_Subscribers
					
					LEFT JOIN tblOCS_Subscribers
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_Subscribers.OCS_Subscriber

					LEFT JOIN tblOCS_UserGroupMembers
					ON tblVxView_Subscribers.VxView_Subscriber = ltrim(tblOCS_UserGroupMembers.OCS_Subscriber, '00')

					LEFT JOIN tblOCS_CUGPackage
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_CUGPackage.OCS_Subscriber

					LEFT JOIN tblOCS_CUGPackageGroupID
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_CUGPackageGroupID.OCS_Subscriber

					WHERE VxView_Subscriber NOT IN (SELECT ltrim(OCS_Subscriber, '00') FROM tblOCS_UserGroupMembers)
					AND OCS_OfferID IN ('33', '34', '36', '37', '38', '39', '43', '44', '45', '46', '47', '48', '49', '55', '1000', '1001', '1002', '1003', '1004', '2000', '2001', '2002', '2003', '2004')

					GROUP BY VxView_Subscriber, VxView_GroupID, tblOCS_UserGroupMembers.OCS_GroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID
					ORDER BY VxView_Subscriber, VxView_GroupID, tblOCS_UserGroupMembers.OCS_GroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID
					
					'''  
		cursor.execute(read_Missing_Subscribers_sqlquery)
		connectionDB.commit()

		counter = 0

		for i in cursor.fetchall():

			counter = counter + 1

			VxView_Subscriber = i[0]
			VxView_GroupID = i[1]

			print(i[0], i[1], i[2], i[3], i[4], i[5])

			cug_config_CreateWrite(VxView_Subscriber, VxView_GroupID)

		print("Total Subscribers without UserGroupMembers GroupID on OCS:", counter)
		print("-------------------------------------------------------------------")
	
	except Exception as e:

		print(e)

def readDB_OCS_UserGroupMembersGroupID_noMatch():
	
	try:

		readUserGroupMembersGroupID_noMatch_sqlquery = '''
				
					SELECT DISTINCT VxView_Subscriber, VxView_GroupID, OCS_Subscriber, OCS_GroupID
					FROM tblVxView_Subscribers

					LEFT JOIN tblOCS_UserGroupMembers
					ON tblVxView_Subscribers.VxView_Subscriber = ltrim(tblOCS_UserGroupMembers.OCS_Subscriber, '00')

					WHERE VxView_Subscriber IN (SELECT ltrim(OCS_Subscriber, '00') FROM tblOCS_UserGroupMembers)
					AND VxView_GroupID <> OCS_GroupID

					GROUP BY VxView_Subscriber, VxView_GroupID, OCS_Subscriber, OCS_GroupID
					ORDER BY VxView_Subscriber, VxView_GroupID, OCS_Subscriber, OCS_GroupID
					
					'''  
		cursor.execute(readUserGroupMembersGroupID_noMatch_sqlquery)
		connectionDB.commit()

		counter = 0

		for i in cursor.fetchall():

			counter = counter + 1
			print(i[0], i[1], i[2], i[3])

			VxView_Subscriber = i[0]
			OCS_GroupID = i[3]

			cug_config_DeleteUserGroupMembersGroupID(VxView_Subscriber, OCS_GroupID)

		print("VxView GroupID vs OCS UserGroupMembership GroupID (not MATCHing records):", counter)
		print("-------------------------------------------------------------------")
	
	except Exception as e:

		print(e)

def readDB_OCS_CUGPackageGroupID_noMatch():
	
	try:

		readCUGPackageGroupID_noMatch_sqlquery = ('''
				
					SELECT DISTINCT OCS_Subscriber, OCS_GroupID, VxView_Subscriber, CUG_Package, VxView_GroupID

					FROM tblOCS_CUGPackageGroupID

					LEFT JOIN tblVxView_Subscribers
					ON VxView_Subscriber = OCS_Subscriber

					WHERE VxView_Subscriber = OCS_Subscriber
					AND VxView_GroupID <> OCS_GroupID

					GROUP BY OCS_Subscriber, OCS_GroupID, VxView_Subscriber, CUG_Package, VxView_GroupID
					ORDER BY OCS_Subscriber, OCS_GroupID, VxView_Subscriber, CUG_Package, VxView_GroupID

					''')
		
		cursor.execute(readCUGPackageGroupID_noMatch_sqlquery)

		counter = 0
		
		for i in cursor.fetchall():

			counter = counter + 1
			print(i[0], i[1], i[2], i[3], i[4])

			VxView_Subscriber = i[0]
			CUG_Package = i[3]
			OCS_GroupID = i[4]

			cug_config_ModifyPackageItem_Delete(VxView_Subscriber, CUG_Package, OCS_GroupID)
		
		print("VxView GroupID vs OCS CUG Package GroupID (not MATCHing records):", counter)
		print("-----------------------------------------------------------")
	
	except Exception as e:

		print(e)

def readDB_Missing_CUGPackage():
	
	try:

		read_Missing_CUGPackage_sqlquery = '''
					
					SELECT DISTINCT VxView_Subscriber, VxView_GroupID, OCS_OfferID, tblOCS_UserGroupMembers.OCS_GroupID AS UserGroupMembersGroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID AS CUGPACKAGE_GROUPID
					FROM tblVxView_Subscribers
					
					LEFT JOIN tblOCS_Subscribers
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_Subscribers.OCS_Subscriber

					LEFT JOIN tblOCS_UserGroupMembers
					ON tblVxView_Subscribers.VxView_Subscriber = ltrim(tblOCS_UserGroupMembers.OCS_Subscriber, '00')

					LEFT JOIN tblOCS_CUGPackage
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_CUGPackage.OCS_Subscriber

					LEFT JOIN tblOCS_CUGPackageGroupID
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_CUGPackageGroupID.OCS_Subscriber

					WHERE VxView_Subscriber NOT IN (SELECT OCS_Subscriber FROM tblOCS_CUGPackage)
					AND OCS_OfferID IN ('33', '34', '36', '37', '38', '39', '43', '44', '45', '46', '47', '48', '49', '55', '1000', '1001', '1002', '1003', '1004', '2000', '2001', '2002', '2003', '2004')

					GROUP BY VxView_Subscriber, VxView_GroupID, tblOCS_UserGroupMembers.OCS_GroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID
					ORDER BY VxView_Subscriber, VxView_GroupID, tblOCS_UserGroupMembers.OCS_GroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID
					
					'''  
		cursor.execute(read_Missing_CUGPackage_sqlquery)
		connectionDB.commit()

		counter = 0

		for i in cursor.fetchall():

			counter = counter + 1
			print(i[0], i[1], i[2], i[3], i[4], i[5])

			VxView_Subscriber = i[0]
			OCS_GroupID = i[1]

			if i[2] in ('33', '34', '36', '37', '38', '39', '43', '44', '45', '46', '47', '48', '49', '55'):

				CUG_Package = 'CUG_500'
			
			if i[2] in ('1000', '1001', '1002', '1003', '1004', '2000', '2001', '2002', '2003', '2004'):

				CUG_Package = 'CUG'

			cug_config_CUGPackage_Subscribe(VxView_Subscriber, CUG_Package)
			cug_config_ModifyPackageItem_Append(VxView_Subscriber, CUG_Package, OCS_GroupID)

		print("Total Subscribers without CUG Package on OCS:", counter)
		print("------------------------------------------------------")
	
	except Exception as e:

		print(e)

def readDB_Missing_CUG_PACKAGE_GroupID():
	
	try:

		read_Missing_CUG_PACKAGE_GroupID_sqlquery = '''
					
					SELECT DISTINCT VxView_Subscriber, VxView_GroupID, OCS_OfferID, tblOCS_UserGroupMembers.OCS_GroupID AS UserGroupMembersGroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID AS CUGPACKAGE_GROUPID
					FROM tblVxView_Subscribers
					
					LEFT JOIN tblOCS_Subscribers
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_Subscribers.OCS_Subscriber

					LEFT JOIN tblOCS_UserGroupMembers
					ON tblVxView_Subscribers.VxView_Subscriber = ltrim(tblOCS_UserGroupMembers.OCS_Subscriber, '00')

					LEFT JOIN tblOCS_CUGPackage
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_CUGPackage.OCS_Subscriber

					LEFT JOIN tblOCS_CUGPackageGroupID
					ON tblVxView_Subscribers.VxView_Subscriber = tblOCS_CUGPackageGroupID.OCS_Subscriber

					WHERE VxView_Subscriber IN (SELECT OCS_Subscriber FROM tblOCS_CUGPackage)
					AND VxView_Subscriber NOT IN (SELECT OCS_Subscriber FROM tblOCS_CUGPackageGroupID)
					AND OCS_OfferID IN ('33', '34', '36', '37', '38', '39', '43', '44', '45', '46', '47', '48', '49', '55', '1000', '1001', '1002', '1003', '1004', '2000', '2001', '2002', '2003', '2004')

					GROUP BY VxView_Subscriber, VxView_GroupID, OCS_OfferID, tblOCS_UserGroupMembers.OCS_GroupID, tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID
					ORDER BY tblOCS_CUGPackage.CUG_Package, tblOCS_CUGPackageGroupID.OCS_GroupID, VxView_Subscriber, VxView_GroupID, OCS_OfferID, tblOCS_UserGroupMembers.OCS_GroupID
					
					'''  
		cursor.execute(read_Missing_CUG_PACKAGE_GroupID_sqlquery)
		connectionDB.commit()

		counter = 0

		for i in cursor.fetchall():

			counter = counter + 1
			print(i[0], i[1], i[2], i[3], i[4], i[5])

			VxView_Subscriber = i[0]
			CUG_Package = i[4]
			OCS_GroupID = i[1]

			cug_config_ModifyPackageItem_Append(VxView_Subscriber, CUG_Package, OCS_GroupID)

		print("Total Subscribers without GroupID on CUG Package on OCS:", counter)
		print("-----------------------------------------------------------------")
	
	except Exception as e:

		print(e)

def cug_config_DeleteUserGroupMembersGroupAll(VxView_Subscriber):

	try:

		global batch_command

		batch_command = '''CUG::Delete:UserGroupMembers(MSISDN); ("00'''+ VxView_Subscriber + '''");'''

		create_batch_commands(VxView_Subscriber)

	except Exception as e:
		print(e)

def cug_config_DeleteUserGroupMembersGroupID(VxView_Subscriber, OCS_GroupID):

	try:

		global batch_command

		batch_command = '''CUG::Delete(UserGroupMembers={MSISDN="00''' + VxView_Subscriber + '''",GroupId=''' + OCS_GroupID + '''});'''

		create_batch_commands(VxView_Subscriber)

	except Exception as e:

		print(e)

def cug_config_ModifyPackageItem_Delete(VxView_Subscriber, CUG_Package, OCS_GroupID):

	try:

		global batch_command

		batch_command = '''CA::Modify:PackageItem(CustomerId="''' + VxView_Subscriber + '''",Package="'''+ CUG_Package + '''",CUGList:delete=''' + OCS_GroupID + ''');'''

		create_batch_commands(VxView_Subscriber)

	except Exception as e:
		print(e)

def cug_config_CUGPackage_Subscribe(VxView_Subscriber, CUG_Package):

	try:

		global batch_command

		batch_command = '''CA::Subscribe:PackageItem(AccessKey="''' + VxView_Subscriber + '''", Package="''' + CUG_Package + '''", ChargeMode=0);'''

		create_batch_commands(VxView_Subscriber)

	except Exception as e:
		print(e)

def cug_config_CUGPackage_Unsubscribe(VxView_Subscriber, CUG_Package):

	try:

		global batch_command

		batch_command = '''CA::Unsubscribe:PackageItem(AccessKey="''' + VxView_Subscriber + '''", Package="''' + CUG_Package + '''", ChargeMode=0);'''

		create_batch_commands(VxView_Subscriber)

	except Exception as e:
		print(e)

def cug_config_ModifyPackageItem_Append(VxView_Subscriber, CUG_Package, OCS_GroupID):

	try:

		global batch_command

		batch_command = '''CA::Modify:PackageItem(CustomerId="''' + VxView_Subscriber + '''",Package="'''+ CUG_Package + '''",CUGList:append=''' + OCS_GroupID + ''');'''

		create_batch_commands(VxView_Subscriber)

	except Exception as e:
		print(e)

def cug_config_CreateWrite(VxView_Subscriber, VxView_GroupID):

	try:

		global batch_command

		batch_command = '''CUG::CreateOrWrite(UserGroupMembers={MSISDN="00''' + VxView_Subscriber + '''",GroupId=''' + VxView_GroupID + '''});'''

		create_batch_commands(VxView_Subscriber)

	except Exception as e:
		print(e)

def ocs_import_data():

	try:

		OCS_outputfile_RetrieveOfferID()
		OCS_outputfile_Retrieve_UserGroupMembershiptable()
		OCS_outputfile_Retrieve_CUGPackage()
		OCS_outputfile_Retrieve_CUGPackageGroupID()

	except Exception as e:

		print(e)

def init_read_data():# This module will start all modules that will read data from the Database.

	try:

		'''
		Missing and Invalid Subscribers should have the CUG configurations removed on OCS.
		Note: it is possible to have deleted subscribers on OCS part of UserGroupMembership table.
		'''

	#	readDB_VxView_Subscribers()
	#	readDB_OCS_all_data()# Reads subscribers information on OCS

	#	readDB_OCS_Subscribers()
	#	readDB_OCS_UserGroupMembers()
	#	readDB_OCS_CUGPackage()
	#	readDB_OCS_CUGPackageGroupID()
			
		readDB_Invalid_Subscribers()
		readDB_Missing_Subscribers()		
		readDB_Missing_UserGroupMembers()
		readDB_Missing_CUGPackage()
		readDB_Missing_CUG_PACKAGE_GroupID()

		readDB_OCS_UserGroupMembersGroupID_noMatch()
		readDB_OCS_CUGPackageGroupID_noMatch()
	#	readDB_compare_profiles()

	except Exception as e:
		print(e)

if __name__ == "__main__":

	DataBase_Connection()

	Read_VxView_Dump()

	ocs_import_data()
	init_read_data()

#	desConnectDB()