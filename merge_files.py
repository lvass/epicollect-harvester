# -*- coding: utf-8 -*-
"""
Created on Fri Nov  1 17:10:14 2019

@author: lv13916
"""


try:
    import pandas as pd
    import glob
    from datetime import datetime 
    import argparse
    import pandas as pd
    import numpy as np
    import sys
    sys.path.append('..')
    import re
    import os
    from datetime import datetime
    import csv
    import time
    import email, smtplib, ssl
    from email import encoders
    from email.mime.base import MIMEBase
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    
    exec(open('C:/local_files/phd_scripts/oh_dart/my_epicollect_functions.py').read())
    exec(open('C:/local_files/phd_scripts/oh_dart/merge_and_combine_functions.py').read())

 
except ModuleNotFoundError: 
    print("Correct modules not found, check requirements are met. Needs:")
    print("argparse, glob, panda, sys, pyepicollect, re, os, datetime, numpy, csv, my_epicollect_functions.py, merge_and_combine_functions.py")
    print("Exiting...")
    exit()
    
print("Combine and merge program has loaded correctly")
    
path = "\\\\rdsfcifs.acrc.bris.ac.uk\\OH_DART_one\\epicollect_api_data\\" 
csv_path = "\\\\rdsfcifs.acrc.bris.ac.uk\\OH_DART_one\\epicollect_api_data" + os.sep + 'csv'
concat_csv_path = "\\\\rdsfcifs.acrc.bris.ac.uk\\OH_DART_one\\epicollect_api_data" + os.sep + "concat_csv"
combined_csv_path = "\\\\rdsfcifs.acrc.bris.ac.uk\\OH_DART_one\\epicollect_api_data" + os.sep + "combined_csv"
branch_combine_csv_path = "\\\\rdsfcifs.acrc.bris.ac.uk\\OH_DART_one\\epicollect_api_data" + os.sep + "branch_merges"

#creating command line options
ap = argparse.ArgumentParser()
ap.add_argument("-o", "--option", help="Type of merge to carry out. Options are: \n rollback_concat (concats all historic csv files), \n concat_latest (concats the latest csv file to the merged file), \n branch_merge (links the merged file for each form via the epicollect ID) \n merge_projects (combines forms based on sample ID, colony ID, site ID etc)", type=str, default = None)
ap.add_argument("-p", "--project", help="Project slug.", type=str, default = "all")
ap.add_argument("-a", "--alert", help="Email address to send alerts to. Default is lv13916@bristol.ac.uk and lucyjvass@gmail.com", type=list, default = ['lv13916@bristol.ac.uk', 'lucyjvass@gmail.com'])
ap.add_argument("-u", "--update", help="Email address to updates to. Default is lv13916@bristol.ac.uk and bivcb@bristol.ac.uk (Ginny Gould)", type=list, default = ['lv13916@bristol.ac.uk', 'bivcb@bristol.ac.uk', 'lucyjvass@gmail.com'])

args = ap.parse_args()

alert_email_addresses = args.alert
update_email_addresses = args.update

# attempt to access RDSF via VPN
# 1 = false, 0 = true
success = 1 #successfuly connection, starts as false
email_sent = 1 #email sent starts as false
counter = 0 # counter of trys
email_counter = 0 #counter of emails sent

while success == 1:
    try:
        test = os.listdir("\\\\rdsfcifs.acrc.bris.ac.uk\\OH_DART_one\\epicollect_api_data")
        print("Successfully connected to RDSF via VPN")
        send_email_alert(subject = "Connected to RDSF, starting download from EpiCollect", body = "Successful connection, see run log and summary email for more info after run",
                         receiver_emails = alert_email_addresses)
        success = 0 # get out of while loop
        
    except (FileNotFoundError):
        success = 1
        print("Not connected to RDSF via VPN")
        # if not a success... 
        # Notify and send email 1st time
        if email_sent == 1:
            send_email_alert(subject = "Can't connect to VPN - merge_files.py", body = "Sign into UoB VPN via F5 Big IP Edge client to allow download of OH-DART data", receiver_emails = [alert_email_addresses])
            print("Sent connection request email")
            email_sent = 0
            email_counter += 1
            time.sleep(5*60) # checks for a connection every 5 minutes
    counter +=1    # counts number of checks
    if counter > 12: # after an hour, fail and send email
        print("VPN connection not authenticated after 1 hour")
        send_email_alert(subject = "VPN connection failure - merge_files.py", body = "No VPN authentication after 8 hours, program will stop trying to get EpiCollect data. \nManually run latest concat and branch merge",
                         receiver_emails = alert_email_addresses)
        exit(message = "VPN connection failure")

              
# carrying out action
if args.option == None:
    print("Must select an option to define the type of action carried out")
    print("Exiting...")
    exit()

if args.option == "rollback_concat":
    message, success = rollback_concat(path, csv_path, concat_csv_path, args.project) 
    print(message)
    if success == True:
        print("Successful rollback_concat, see " + concat_csv_path + " for resulting csv")
        send_email_alert(subject = "Successful rollback_concat", body = "Successful rollback_concat, see " + concat_csv_path + " for resulting csv",
                         receiver_emails = alert_email_addresses)
    else:
        print("Errors exist - unsuccessful rollback_concat, see above for errors")
        send_email_alert(subject = "Unsuccessful rollback_concat", body = "Errors exist - unsuccessful rollback_concat \n" + message,
                         receiver_emails = alert_email_addresses)
    print("Exiting...")
    exit()
    
if args.option == "latest_concat":
    message, success = latest_concat(path, csv_path, concat_csv_path, args.project)
    print(message)
    if success == True:
        print("Successful latest_concat, see " + concat_csv_path + " for resulting csv")
        send_email_alert(subject = "Successful latest_concat", body = "Successful latest_concat, see " + concat_csv_path + " for resulting csv",
                         receiver_emails = alert_email_addresses)
    else:
        print("Errors exist - unsuccessful latest_concat, see above for errors")
        send_email_alert(subject = "Unsuccessful latest_concat", body = "Errors exist - unsuccessful latest_concat \n" + message,
                         receiver_emails = update_email_addresses)
    
    print("Exiting...")
    exit()
    
if args.option == "branch_merge":
    error_msg, error_count, warn_msg, warn_count = branch_merge(path, csv_path, concat_csv_path, branch_combine_csv_path, args.project) 
    #error_msg, error_count, warn_msg, warn_count = branch_merge(path, csv_path, concat_csv_path, branch_combine_csv_path, "all")
    
    #save run log
    #write the user message to a run log .txt
    with open(path + os.sep + "run_logs" + os.sep +'branch_merge_'+ str(datetime.now()).replace(":", ".").replace(" ", "T")[0:-7] + '.txt', 'w', newline='') as f: 
        f.write(str(error_count) + " errors and " + str(warn_count) + " warnings exist.")    
        f.write("\n" + error_msg)
        f.write("\n" + warn_msg)
        f.close()
            
    if error_count == 0:
        print("Successful branch_merge, see " + branch_combine_csv_path + " for resulting csv files")
        send_email_alert(subject = "Successful branch_merge", body = "Successful branch_merge, see " + branch_combine_csv_path + " for resulting csv files",
                         receiver_emails = alert_email_addresses)
    else:
        print(str(error_count) + " errors and " + str(warn_count) + " warnings exist. See run log for errors.")
        send_email_alert(subject = "Unsuccessful branch_merge", body = str(error_count) + " errors and " + str(warn_count) + " warnings exist. See run log for errors.",
                         receiver_emails = update_email_addresses)
    print("Exiting...")
    exit()
    
if args.option == "merge_projects":
    print("Not yet coded")
    exit()
    

        
        



    



    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    