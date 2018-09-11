import xlsxwriter
import os, sys, re, csv
import openpyxl, threading, multiprocessing
from openpyxl import load_workbook
import time, shutil, datetime, logging, locale, os, errno

from cugOCSDB import *
from cug_config_file import *
from cug_import_export_data import *
from ReadOCS_output_Data import *

def init_cug_tool():

	try:
		
		Credentials()

		if (os.path.isfile(VxView_CUG_Dump)):

			ssh_OCS_DataCleaner()

			return

			Create_OCS_Read_Commands()
		
		

		if (os.path.isfile(txt_OCS1_ReadCUG_Config) or os.path.isfile(txt_OCS3_ReadCUG_Config)):

			send_batch_command_file()

		if (os.path.isfile(txt_processed_OCS1_ReadCUG_Config) or os.path.isfile(txt_processed_OCS3_ReadCUG_Config)):

			process_Import_VxView_Dump = multiprocessing.Process(target = Import_VxView_Dump)
			process_Import_VxView_Dump.start()
			process_Import_VxView_Dump.join()
			time.sleep(3)

		while not (os.path.isfile(OCS_CUG_output_file)):

			ssh_OCS_DataCleaner()
			receive_output_file()
		
		if (os.path.isfile(OCS_CUG_output_file)):
			
			Import_OCS_output_Data()

		if (os.path.isfile(cug_report)):

			Create_Report()

		if (os.path.isfile(cug_report)):

			SendEmail()

	except Exception as e:
		print(e)	

if __name__ == '__main__':

#	connectDB()

	init_cug_tool() #For looping while I go have some bath.

#	desConnectDB()