# from pyspark.sql import SparkSession
# from pyspark.sql.functions import split
# import csv
# import os
#
# inFile = os.path.join(os.getcwd(), 'Sample_Data_CsvDOS.csv')
# outFile = os.path.join(os.getcwd(), 'Sample_Data_CsvDOS_out.csv')
#
# # Start a spark session
# spark = SparkSession.builder.appName('Basics').getOrCreate()
#
# # Read .csv file and create a DataFrame
# df = spark.read.csv(inFile, inferSchema=True, header=True)
#
# df.show()
#
# # Collect data from file
# result = df.select(split("Temperature value", ',').alias('TEMP_VAL')).collect()
#
# # Temporary list of channels with temperatures between 4 - 7 degrees
# deg_list = []
#
# with open(outFile, 'w', newline='') as outCsvFile:
#     writer = csv.writer(outCsvFile)
#     writer.writerow(['PORT_NAME', 'MIN_TEMP', 'MAX_TEMP', 'AVG_TEMP'])
# outCsvFile.close()
#
# # Create query with selected data
# for i in range(len(result)):
#     # Temporary list with values for each channel
#     temp = []
#
#     # Go through temperature values from each channel
#     for idx in range(len(result[i][0])):
#         # Convert string type variable to float for doing operations
#         temp.append(float(result[i][0][idx]))
#
#     if min(temp) >= -4.0 and max(temp) <= 10.0:
#         deg_list.append('TSM-{0}'.format(i+1))
#
#     # csv.register_dialect('customDialect', delimiter='\t')
#     # Create "port name.csv" log file
#     with open(outFile, 'a', newline='') as csvfile:
#         # define table fields
#         fieldnames = ['PORT_NAME', 'MIN_TEMP', 'MAX_TEMP', 'AVG_TEMP']
#         # create dictionary writer object with defined fields
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         # write values in fields
#         writer.writerow({'PORT_NAME': 'TSM-{}'.format(i+1), 'MIN_TEMP': min(temp), 'MAX_TEMP': max(temp),
#                          'AVG_TEMP': sum(temp)/len(temp)})
#     csvfile.close()
#
#     # Print to console Min , Max and Avg temperature value for each channel
#     print('TSM-{0}: \n Min_temp: {1} \n Max_temp: {2} \n Avg_temp: {3} \n'.format(i+1, min(temp), max(temp),
#                                                                                   sum(temp)/len(temp)))
#
# with open(outFile, 'a', newline='') as outCsvFile:
#     writer = csv.writer(outCsvFile)
#     writer.writerow([])
#     writer.writerow(['Channel with temperatures within -4.0 <-> +10.0 degrees: {0}'.format(deg_list)])
#
# # Print list of channels
# print('Channel with temperatures within -4.0 <-> +10.0 degrees: {0}'.format(deg_list))
#
#

# """
# *****************************************************************************
# *                                                                           *
# *                   Copyright 2017 (c) TELETEL S.A.                         *
# *                                                                           *
# *****************************************************************************
# *                                                                           *
# *                             IDENTIFICATION                                *
# *                                                                           *
# *  Project name           :    IMA Separation Kernel Qualification Campaign	*
# *																			*
# *  Description            :	   Test Case DR-4, Invalid API Calls		    *
# *                                                                           *
# *****************************************************************************
#
# """
#
# # Test Case: DR-4, Invalid API Calls
# #
# # Description: This test explores any API call or interface to the API where
# # it’s possible to call the API at the boundary of the partition address.
# #
# # Associated Requirements: No applicable requirements
# #
# # Preconditions: 1.The API should be accessible via suitable test agent
# #                2. The API should be exposed without additional protection or wrapping
#
# import unittest
#
# from Configurations import Constants
# from Configurations.Constants import *
# from TestSupport import TestLib
# from TestSupport.TLogger import *
import pyautogui, time
#
# # Partition start address
# PARTITION_SA = {"0": 0x40000000,  # size="4MB"
#                 "1": 0x401A0000,  # size="128kb"
#                 "2": 0x401C0000,  # size="128kb"
#                 "3": 0x401e0000}  # size="128kb"
#
#
# class InvalidAPICalls(unittest.TestCase):
#     # Set test name for retrieving info
#     test_name = 'DR4_1'
#
#     # Display title, objective, requirements coverage
#     TLogger.TestTitle('DR-4', 'Invalid API Calls')
#     TLogger.TestDesc("This test explores any API call or interface to the API where it’s "
#                      "possible to call the API at the boundary of the partition address.")
#     TLogger.TestRequirements("No applicable requirements")
#     TLogger.TestPreconditions("\n1. The API should be accessible via suitable test agent\n"
#                               "2. The API should be exposed without additional protection or wrapping\n")
#
#     time.sleep(2)
#
#     def testDR4_1(self):
#         log_object = TLogger()
#         TLib_object = TestLib.TestLib
#
#         # Test Configurations
#         # Test message used in API calls
#         message = 'hello'
#
#         # Test partition ID for executing tests commands
#         executing_pid = 1
#
#         # Partition ID used in API parameters
#         partition_id = 1
#
#         # Image of partitions software
#         partition_image = 'resident_sw'
#
#         # Initialization settings of the system
#         options = Configurations.INIT_OPTIONS
#
#         # End of Test Configurations
#
#         # Initialize system
#         res = TLib_object.TsimInit(options)
#         # Check if the system initialization returned no error
#         self.assertEqual(0, res, "TsimInit executed with no error (error code = {} ).".format(res))
#
#         # Load system configuration
#         res = TLib_object.TsimLoad(partition_image)
#         # Check if loading system configuration file returned no error
#         self.assertEqual(0, res, "TsimLoad executed with no error (error code = {} ).".format(res))
#
#         # Run system
#         res = TLib_object.TsimGo()
#         # Check if system start returned no error
#         self.assertEqual(0, res, "TsimGo executed with no error (error code = {} ).".format(res))
#
#         # Step 1
#         log_object.StepTitle("1.", "Partition 1: Call the API functions with valid parameters.")
#         log_object.StepRequirements("No applicable requirements")
#         time.sleep(0.1)
#
#         # I hope this is correct as it is still a little unclear to me the concept of memory pointer accessing and
#         # writing
#         # For example:
#         # array[4096]
#         # message_size = 64 bits
#         # buff = array + array[4094] * 8 -> in this case 32 bits will be written in array[4094]
#         # and another 32bits in array[4095]?
#         # what if  buff = array + array[4094] * 7 -> 3 bytes will be written in array[4094],
#         # 4 bytes in array[4095] and 1 byte outside of the buffer?
#
#         # get the valid pointer with 4096 cells of 4 bytes each
#         # pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # calculate memory space to write on the buffer
#         # E.g:
#         # KernelGetTime writes a value of 64 bit (8 bytes) so length = 8
#         # To be able to write at the boundary of the valid pointer we use the
#         # following equation:
#         # buffer = pointer[3] + (4096 - size//4) * (size - 1)
#         # pointer[3] -> start of the first 4 byte cell, of the 4096 cells array
#         # 4096 -> size of the array
#         # size -> size of the message in bytes
#         # Example:
#         # message = 'hello'
#         # mesage_size = 5 bytes => buffer = pointer[3] + (4095 - 5//4) * (5 -1) => 4 bytes will
#         # be written inside the buffer and 1 byte outside
#
#         # Step 1.1 KernelGetTime
#         log_object.StepTitle("1.1", "KernelGetTime: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES (type KernelTime_t)
#         size = 8
#         # set boundary address of partition 1
#         time_addr = pointer[3] + (4096 - size//4) * (size - 1)
#         ######## Put the enumeration of possible clocks in the Constants file and use the enum instead of number 0x0
#         # HW_CLOCK (0x0)
#         clock_id = HW_CLOCK
#         res = TLib_object.KernelGetTime(executing_pid, clock_id, time_addr)
#
#         log_object.StepExpect("KernelGetTime should be executed successfully and return NO ERROR")
#         ###### Use a wider enumeration , OK-> ANSW_OK, NOK->ANSW_NOK in Constants.py
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # # Step 1.2 KernelGetPartitionMmap
#         # # KernelAPIAdapter.c - "Get Partition MMAP function is not supported by XtratuM-Q version."
#         # # get partition 1 valid pointer
#         # VALID_ADDR = TLib_object.KernelGetValidPtr(executing_pid)
#         # mmap_address = VALID_ADDR[3]
pyautogui.PAUSE = 1
pyautogui.FAILSAFE = True
#         # log_object.StepTitle("1.2", "Call KernelGetPartitionMmap and check that it returns SUCCESS.")
#         # res = TLib_object.KernelGetPartitionMmap(executing_pid, mmap_address)
#         # log_object.StepExpect("KernelGetPartitionMmap should return SUCCESS")
#         # self.assertEqual(ANSW_OK, res[0], "Function expected 'OK', got {}.".format(res[0]))
#         # # NO_ERROR = SUCCESS (0)
while True:
    pyautogui.scroll(850)
    pyautogui.moveRel(100, 0, duration=1)
    # pyautogui.click(1000, 1000, button='left')
    pyautogui.moveRel(100, 200, duration=1)
    pyautogui.moveRel(0, 100, duration=1)
    pyautogui.keyDown('shift')
    time.sleep(1)
    pyautogui.keyUp('shift')
    # pyautogui.click(1000, 1000, button='left')
    pyautogui.moveRel(-100, 0, duration=1)
    pyautogui.moveRel(-100, -200, duration=1)
    pyautogui.moveRel(0, -100, duration=1)
    # pyautogui.click(1000, 1000, button='left')
    pyautogui.scroll(-900)
#         # # error_code[int(res[2])] = ERROR CODE (see error code dictionary in Constants.py)
#         # self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#         #                  format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.2 KernelInPort
#         log_object.StepTitle("1.2", "KernelInPort: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES
#         size = len(message)
#         # set boundary address of partition 1
#         port_address = pointer[3] + (4096 - size // 4) * (size - 1)
#         # message value to write in the buffer
#         value = message
#
#         res = TLib_object.KernelInPort(executing_pid, port_address, value)
#
#         log_object.StepExpect("KernelInPort should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.3 KernelOutPort
#         log_object.StepTitle("1.3", "KernelOutPort: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES
#         size = len(message)
#         # set boundary address of partition 1
#         port_address = pointer[3] + (4096 - size//4) * (size - 1)
#         # message value to write in the buffer
#         value = message
#
#         res = TLib_object.KernelOutPort(executing_pid, port_address, value)
#
#         log_object.StepExpect("KernelOutPort should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.4 KernelGetSystemStatus
#         log_object.StepTitle("1.4", "KernelGetSystemStatus: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES (type KernelSystemStatus_t)
#         size = 76
#         # set boundary address of partition 1
#         status_address = pointer[3] + (4096 - size//4) * (size - 1)
#
#         res = TLib_object.KernelGetSystemStatus(executing_pid, status_address)
#
#         log_object.StepExpect("KernelGetSystemStatus should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.5 KernelGetPlanStatus
#         log_object.StepTitle("1.5", "KernelGetPlanStatus: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES (type KernelPlanStatus_t)
#         size = 20
#         # set boundary address of partition 1
#         status_address = pointer[3] + (4096 - size//4) * (size - 1)
#
#         res = TLib_object.KernelGetPlanStatus(executing_pid, status_address)
#
#         log_object.StepExpect("KernelGetPlanStatus should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.6 KernelGetPartitionStatus
#         log_object.StepTitle("1.6", "KernelGetPartitionStatus: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES (type KernelPartitionStatus_t)
#         size = 60
#         # set boundary address of partition 1
#         status_address = pointer[3] + (4096 - size//4) * (size - 1)
#
#         res = TLib_object.KernelGetPartitionStatus(executing_pid, partition_id, status_address)
#
#         log_object.StepExpect("KernelGetPartitionStatus should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Call KernelCreateQueuingPort to get port description for functions KernelGetQueuingPortStatus,
#         # KernelReadQueuingMessage, KernelWriteQueuingMessage
#         port_name = Configurations.SOURCE_PORT_NAMES[1]
#         max_no_msgs = Configurations.MAX_NO_MSGS
#         max_msg_size = Configurations.MAX_MSG_SIZE
#         direction = Constants.PK_SOURCE_PORT
#         port_desc = TLib_object.KernelCreateQueuingPort(executing_pid, port_name, max_no_msgs,
#                                                         max_msg_size, direction)
#
#         # Step 1.7 KernelGetQueuingPortStatus
#         log_object.StepTitle("1.7", "KernelGetQueuingPortStatus: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES (type KernelQueuingPortStatus_t)
#         size = 4
#         # set boundary address of partition 1
#         status_address = pointer[3] + (4096 - size//4) * (size - 1)
#
#         res = TLib_object.KernelGetQueuingPortStatus(executing_pid, port_desc[3], status_address)
#
#         log_object.StepExpect("KernelGetQueuingPortStatus should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.8 KernelReadQueuingMessage
#         log_object.StepTitle("1.8", "KernelReadQueuingMessage: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES
#         size = 4
#         # set boundary address of partition 1
#         msg_address = pointer[3] + (4096 - size//4) * (size - 1)
#         # message size to be written in bits
#         msg_size = Configurations.MAX_MSG_SIZE
#
#         res = TLib_object.KernelReadQueuingMessage(executing_pid, port_desc, msg_size, msg_address)
#
#         log_object.StepExpect("KernelReadQueuingMessage should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.9 KernelWriteQueuingMessage
#         log_object.StepTitle("1.9", "KernelWriteQueuingMessage: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES
#         size = 4
#         # set boundary address of partition 1
#         msg_address = pointer[3] + (4096 - size//4) * (size - 1)
#         # message size to be written in bits
#         msg_size = Configurations.MAX_MSG_SIZE
#
#         res = TLib_object.KernelWriteQueuingMessage(executing_pid, port_desc, msg_address, msg_size)
#
#         log_object.StepExpect("KernelWriteQueuingMessage should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.10 KernelGetQueuingPortInfo
#         log_object.StepTitle("1.10", "KernelGetQueuingPortInfo: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES (type KernelQueuingPortInfo_t)
#         size = 16
#         # set boundary address of partition 1
#         port_info_address = pointer[3] + (4096 - size//4) * (size - 1)
#         # partition 1 source port name
#         port_name = Configurations.SOURCE_PORT_NAMES[1]
#
#         res = TLib_object.KernelGetQueuingPortInfo(executing_pid, port_name, port_info_address)
#
#         log_object.StepExpect("KernelGetQueuingPortInfo should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Call KernelCreateSamplingPort to get port description for functions KernelGetSamplingPortStatus,
#         # KernelReadSamplingMessage, KernelWriteSamplingMessage
#         outport = Configurations.SOURCE_PORT_NAMES[1]
#         direction = Constants.PK_SOURCE_PORT
#         msg_size = len(message)
#         refresh_period = Configurations.REFRESH_PERIOD
#         port_desc = TLib_object.KernelCreateSamplingPort(executing_pid, outport, msg_size,
#                                                          direction, refresh_period)
#
#         # Step 1.11 KernelGetSamplingPortStatus
#         log_object.StepTitle("1.11", "KernelGetSamplingPortStatus: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES (type KernelSamplingPortStatus_t)
#         size = 16
#         # set boundary address of partition 1
#         status_address = pointer[3] + (4096 - size//4) * (size - 1)
#
#         res = TLib_object.KernelGetSamplingPortStatus(executing_pid, port_desc, status_address)
#
#         log_object.StepExpect("KernelGetSamplingPortStatus should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.12 KernelReadSamplingMessage
#         log_object.StepTitle("1.12", "KernelReadSamplingMessage: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES
#         size = 4
#         # set boundary address of partition 1
#         msg_address = pointer[3] + (4096 - size//4) * (size - 1)
#         # message size to be written in bits
#         msg_size = Configurations.MAX_MSG_SIZE
#
#         res = TLib_object.KernelReadSamplingMessage(executing_pid, port_desc[3], msg_size, msg_address)
#
#         log_object.StepExpect("KernelReadSamplingMessage should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.13 KernelWriteSamplingMessage
#         log_object.StepTitle("1.13", "KernelWriteSamplingMessage: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES
#         size = 4
#         # set boundary address of partition 1
#         msg_address = pointer[3] + (4096 - size//4) * (size - 1)
#         # message size to be written in bits
#         msg_size = Configurations.MAX_MSG_SIZE
#
#         res = TLib_object.KernelWriteSamplingMessage(executing_pid, port_desc[3], msg_address, msg_size)
#
#         log_object.StepExpect("KernelWriteSamplingMessage should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.14 KernelGetSamplingPortInfo
#         log_object.StepTitle("1.14", "KernelGetSamplingPortInfo: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES (type KernelSamplingPortInfo_t)
#         size = 4
#         # set boundary address of partition 1
#         port_info_address = pointer[3] + (4096 - size//4) * (size - 1)
#         # partition 1 source port name
#         port_name = Configurations.SOURCE_PORT_NAMES[1]
#
#         res = TLib_object.KernelGetSamplingPortInfo(executing_pid, port_name, port_info_address)
#
#         log_object.StepExpect("KernelGetSamplingPortInfo should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.15 KernelHmRead
#         log_object.StepTitle("1.15", "KernelHmRead: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES (type KernelHmLog_t)
#         size = 16
#         # set boundary address of partition 1
#         hm_log_ptr_address = pointer[3] + (4096 - size//4) * (size - 1)
#         # number of logs
#         no_logs = 1
#
#         res = TLib_object.KernelHmRead(executing_pid, no_logs, hm_log_ptr_address)
#
#         log_object.StepExpect("KernelHmRead should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.16 KernelHmStatus
#         log_object.StepTitle("1.16", "KernelHmStatus: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES (type KernelHmStatus_t)
#         size = 4
#         # set boundary address of partition 1
#         hm_status_ptr_address = pointer[3] + (4096 - size//4) * (size - 1)
#
#         res = TLib_object.KernelHmStatus(executing_pid, hm_status_ptr_address)
#
#         log_object.StepExpect("KernelHmStatus should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.17 KernelTraceRead
#         log_object.StepTitle("1.17", "KernelTraceRead: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES (type KernelTraceEvent_t)
#         size = 5
#         # set boundary address of partition 1
#         trace_event_ptr_address = pointer[3] + (4096 - size//4) * (size - 1)
#         # number of traces
#         no_traces = 1
#
#         res = TLib_object.KernelTraceRead(executing_pid, partition_id, no_traces, trace_event_ptr_address)
#
#         log_object.StepExpect("KernelTraceRead should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.18 KernelTraceStatus
#         log_object.StepTitle("1.18", "KernelTraceStatus: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES (type KernelTraceStatus_t)
#         size = 4
#         # set boundary address of partition 1
#         trace_status_ptr = pointer[3] + (4096 - size // 4) * (size - 1)
#
#         res = TLib_object.KernelTraceStatus(executing_pid, partition_id, trace_status_ptr)
#
#         log_object.StepExpect("KernelTraceStatus should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.19 KernelTraceEvent
#         log_object.StepTitle("1.19", "KernelTraceRead: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES (type KernelTraceEvent_t)
#         size = 5
#         # set boundary address of partition 1
#         trace_event_address = pointer[3] + (4096 - size // 4) * (size - 1)
#
#         res = TLib_object.KernelTraceEvent(executing_pid, trace_event_address)
#
#         log_object.StepExpect("KernelTraceEvent should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.20 KernelParamsGetPCT
#         log_object.StepTitle("1.20", "KernelParamsGetPCT: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES (type KernelPartitionControlTable_t)
#         size = 68
#         # set boundary address of partition 1
#         pct_pointer_address = pointer[3] + (4096 - size // 4) * (size - 1)
#
#         res = TLib_object.KernelParamsGetPCT(executing_pid, pct_pointer_address)
#
#         log_object.StepExpect("KernelParamsGetPCT should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.21 KernelReadPtr
#         log_object.StepTitle("1.21", "KernelReadPtr: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES # TBD
#         size = 4
#         # set boundary address of partition 1
#         mem_address = pointer[3] + (4096 - size // 4) * (size - 1)
#         # message size to be written in bits
#         msg_size = Configurations.MAX_MSG_SIZE
#
#         res = TLib_object.KernelReadPtr(executing_pid, mem_address, msg_size)
#
#         log_object.StepExpect("KernelReadPtr should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.22 KernelWritePtr
#         log_object.StepTitle("1.22", "KernelWritePtr: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES # TBD
#         size = 4
#         # set boundary address of partition 1
#         mem_address = pointer[3] + (4096 - size // 4) * (size - 1)
#         # message size to be written in bits
#         msg_size = Configurations.MAX_MSG_SIZE
#         # message value to write in the buffer
#         value = message
#
#         res = TLib_object.KernelWritePtr(executing_pid, mem_address, msg_size, value)
#
#         log_object.StepExpect("KernelWritePtr should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.23 KernelReadAddress
#         log_object.StepTitle("1.23", "KernelReadAddress: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES # TBD
#         size = 4
#         # set boundary address of partition 1
#         address = pointer[3] + (4096 - size // 4) * (size - 1)
#
#         res = TLib_object.KernelReadAddress(executing_pid, address, mask=None)
#
#         log_object.StepExpect("KernelReadAddress should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.24 KernelWriteAddress
#         log_object.StepTitle("1.24", "KernelWriteAddress: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES # TBD
#         size = 4
#         # set boundary address of partition 1
#         address = pointer[3] + (4096 - size // 4) * (size - 1)
#         # message value to write in the buffer
#         value = message
#
#         res = TLib_object.KernelWriteAddress(executing_pid, address, value, mask=None)
#         log_object.StepExpect("KernelWriteAddress should return SUCCESS")
#
#         log_object.StepExpect("KernelWriteAddress should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         # Step 1.26 KernelExecAddress
#         log_object.StepTitle("1.23", "KernelReadAddress: call it at partition 1 with valid pointer.")
#         # get valid memory pointer of partition 1
#         pointer = TLib_object.KernelGetValidPtr(executing_pid)
#         # size of the message in BYTES # TBD
#         size = 4
#         # set boundary address of partition 1
#         address = pointer[3] + (4096 - size // 4) * (size - 1)
#
#         res = TLib_object.KernelExecAddress(executing_pid, address)
#
#         log_object.StepExpect("KernelExecAddress should be executed successfully and return NO ERROR")
#         self.assertEqual(ANSW_OK, res[0], "Command executed  successfully, answer received {}.".format(res[0]))
#         self.assertEqual(NO_ERROR, int(res[2]), "Function expected {}, got {}.".
#                          format(NO_ERROR, error_code[int(res[2])]))
#
#         self.assertEqual(0, 0, "")
#
#
# if __name__ == '__main__':
#     unittest.main()
