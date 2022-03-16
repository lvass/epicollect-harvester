# -*- coding: utf-8 -*-
"""
Created on Sun Nov  3 14:14:31 2019

@author: lv13916
"""

def send_email_alert(subject, body, receiver_emails):
    """
    Sends email to me with user message and run log
    """
    sender_email = "epicollect5.harvester.report@gmail.com"
    #receiver_emails = ["lv13916@bristol.ac.uk", "lucyjvass@gmail.com"]
    password = "ohdart377"
    COMMASPACE = ', '
    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = COMMASPACE.join(receiver_emails)
    message["Subject"] = subject
    
    # Add body to email
    message.attach(MIMEText(body, "plain"))
    
    # Add attachment to message and convert message to string
    text = message.as_string()
    
    # Log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_emails, text)
        
        
def rollback_concat(path, csv_path, concat_csv_path, project_to_process):
    '''
    Looks at all historic csv files and concats them together into one file with no reptitions
    '''
    message = "Running rollback_concat... "
    
    
    # create dict of projects and checks RDSF connection 
    if(path == '\\\\rdsfcifs.acrc.bris.ac.uk\\OH_DART_one\\epicollect_api_data'):
        
        try:
            project_info_raw = pd.read_csv(path + os.sep + "project_info.csv") 
        except (PermissionError, OSError, FileNotFoundError) as e:
            print("Can't connect to RDSF, can't open project_info.csv")
            print(str(e) + "\nWaiting 1 hour......")
            time.sleep(3600)
            
            # 2nd try an hour later
            try:
                project_info_raw = pd.read_csv(path + os.sep + "project_info.csv") 
            except (PermissionError, OSError, FileNotFoundError) as e:
                msg = "Can't connect to RDSF, can't open project_info.csv, " + str(e)
                print(msg)
                print("\nExiting...")
                send_email_alert("Error connecting to RDSF", msg)
                exit()  
            
    if(path != '\\\\rdsfcifs.acrc.bris.ac.uk\\OH_DART_one\\epicollect_api_data'):
        try:
            project_info_raw = pd.read_csv(path + os.sep + "project_info.csv") 
        except (PermissionError, OSError, FileNotFoundError) as e:
            msg = "Can't find project_info.csv which contains project names and authentication details."
            msg = msg + "\n Trying to access " + path
            msg = msg + "\n Getting error" + str(e)
            print(msg)
            print("\nExiting...")
            exit()   
    
    success = True
        
    project_info_raw = project_info_raw.replace({pd.np.nan: None})
    
    project_in_use = project_info_raw[project_info_raw.in_use == "T"]
    
    # make folder to store concats in
    try:
        os.mkdir(concat_csv_path)
    except FileExistsError:
        pass
        
    if project_to_process != "all":
        project_in_use = project_info_raw[project_info_raw.project_slug == project_to_process]

    project_in_use = project_in_use[project_info_raw.branch_name == "parent"]
    for index, row in project_in_use.iterrows():
     
        folder_name = row['project_slug'] + '_' + row['form_slug'] + '_' + row['branch_name']
        print("Combining files in " + folder_name)
        print("..........")
        #folder_name = "agricultural-samples_parent"   
        file_list = glob.glob(csv_path + os.sep + folder_name + os.sep + '*.csv')
        df_array = []
        i = 0
        n_cols = None
        col_names = None
        pattern = re.compile("\\\\\d{4}-\d{2}-\d{2}(.*)Z")
        
        for file in file_list:
            #print(file)
            #opening file and reading cvs 
            file_data = pd.read_csv(file)
            
            #getting download date from file name
            r = re.search(pattern, file)
            datetime = r.group(0)[1:]
            
            # adding datetime stamp to file records
            file_data['download_date'] = datetime
            
            #removing trailing underscores as these can lead to mismatching cols
            file_data.columns = file_data.columns.str.replace("_$", "")
            
            #check number of cols and names of cols now trailing _ has been replaced
            if i!= 0:
                if file_data.shape[1] != n_cols or sum(col_names == file_data.columns) !=n_cols:
                    print("Warning - mismatch in name or number of columns between csv files")
                    message = message + "\n Error - mismatch in name or number of columns between csv files in " + folder_name
                    success = False
            
            # recording number of cols
            n_cols = file_data.shape[1]    
            col_names = file_data.columns
            #adding data into an array
            df_array.append(file_data)
            #iter
            i = i + 1
        
        if i == 0:
            message = message + "\n No files found for project " + folder_name
        else:
            # concat all together into one big pandas
            concat_data = pd.concat(df_array, axis=0, join='outer', ignore_index=False, keys=None,
                      levels=None, names=None, verify_integrity=False, copy=True, sort = False)
            
            #sort by download date and reindex
            concat_data.sort_values(by=['download_date'])
            concat_data = concat_data.reset_index(drop=True)
            
            # check for duplicates and remove the oldest duplicates    
            cols_to_check = concat_data.columns[:-1] # all but the last, which is the newly created col 'date downloaded'
            dup = concat_data.duplicated(subset = cols_to_check, keep = 'last')  #keep = last : All duplicates except their last occurrence will be marked as True

            dup_index = dup[dup].index
            concat_data = concat_data.drop(dup_index, axis=0)
            
            # save csv
            save_name = concat_csv_path +  os.sep + folder_name + '.csv'
            
            try:
                concat_data.to_csv(save_name, index = False)
                message = message + "\n Saved in " + save_name
            except FileNotFoundError:
                message = message + "\n No such file or directory: " + save_name
                success = False
            except IOError:
                message = message + "\n IOError: Permission denied for path " + save_name
                success = False
            except AttributeError:
                message = message + "\n Attribute error - no data frame to save"
                success = False
        
    return message, success



def latest_concat(path, csv_path, concat_csv_path, project_to_process):
    '''
    concats most recent csv file for each form into the concat master csv
    '''
    message = "Running concat... "
    
    
    # create dict of projects and checks RDSF connection 
    if(path == '\\\\rdsfcifs.acrc.bris.ac.uk\\OH_DART_one\\epicollect_api_data'):
        try:
            project_info_raw = pd.read_csv(path + os.sep + "project_info.csv") 
        except (PermissionError, OSError, FileNotFoundError) as e:
            print("Can't connect to RDSF, can't open project_info.csv")
            print(str(e) + "\nWaiting 1 hour......")
            time.sleep(3600)
            
            # 2nd try an hour later
            try:
                project_info_raw = pd.read_csv(path + os.sep + "project_info.csv") 
            except (PermissionError, OSError, FileNotFoundError) as e:
                msg = "Can't connect to RDSF, can't open project_info.csv, " + str(e)
                print(msg)
                print("\nExiting...")
                send_email_alert("Error connecting to RDSF", msg)
                exit()  
            
    if(path != '\\\\rdsfcifs.acrc.bris.ac.uk\\OH_DART_one\\epicollect_api_data'):
        try:
            project_info_raw = pd.read_csv(path + os.sep + "project_info.csv") 
        except (PermissionError, OSError, FileNotFoundError) as e:
            msg = "Can't find project_info.csv which contains project names and authentication details."
            msg = msg + "\n Trying to access " + path
            msg = msg + "\n Getting error" + str(e)
            print(msg)
            print("\nExiting...")
            exit()   
    
    success = True
        
    project_info_raw = project_info_raw.replace({pd.np.nan: None})
    
    project_in_use = project_info_raw[project_info_raw.in_use == "T"]

    if project_to_process != "all":
        project_in_use = project_info_raw[project_info_raw.project_slug == project_to_process]
        
    pattern = re.compile("\\\\\d{4}-\d{2}-\d{2}(.*)Z")

    for index, row in project_in_use.iterrows():
        folder_name = row['project_slug'] + '_' + row['form_slug'] + '_' + row['branch_name']

        print("Combining files in " + folder_name)
        print("..........")
        # Open correct concat file in concat_csv and put into panda
        concat_master_name = concat_csv_path + os.sep + folder_name +".csv"
        print(concat_master_name)
        try:
            concat_master_data = pd.read_csv(concat_master_name)
        except FileNotFoundError:
            message = message + "\n Couldn't find file " + concat_master_name
            print(message)
            print("\nExiting...")
            exit()

        # Open most recent csv in csv folder
        file_list = glob.glob(csv_path + os.sep + folder_name + os.sep + '*.csv')
        if len(file_list) == 0:
            message = message + "\n No files found for project " + folder_name
            print(message)
            print("\nExiting...")
            exit()
            
        latest_csv = max(file_list, key=os.path.getctime)
        
        try:
            most_recent_data = pd.read_csv(latest_csv)
        except FileNotFoundError:
            message = message + "\n Couldn't find file " + most_recent_data
            print(message)
            print("\nExiting...")
            exit()
            
        #getting download date from file name
        r = re.search(pattern, latest_csv)
        datetime = r.group(0)[1:]
        
        # adding datetime stamp to file records
        most_recent_data['download_date'] = datetime
            
        # removing trailing _
        concat_master_data.columns = concat_master_data.columns.str.replace("_$", "")
        most_recent_data.columns = most_recent_data.columns.str.replace("_$", "")
        
        # prepapre to concat - check cols match
        if concat_master_data.shape[1] != most_recent_data.shape[1] or sum(concat_master_data.columns == most_recent_data.columns) != most_recent_data.shape[1]:
            print("Warning - mismatch in name or number of columns between csv files")
            message = message + "\n Error - mismatch in name or number of columns between csv files in " + folder_name
            success = False
            
        else:
            df_array = [concat_master_data, most_recent_data]
            # concat all together into one big pandas
            concat_data = pd.concat(df_array, axis=0, join='outer', ignore_index=False, keys=None,
                      levels=None, names=None, verify_integrity=False, copy=True, sort = False)
            
            #sort by download date and reindex
            concat_data.sort_values(by=['download_date'])
            concat_data = concat_data.reset_index(drop=True)
            

            # check for duplicates and remove the oldest duplicates    
            cols_to_check = concat_data.columns[:-1] # all but the last, which is the newly created col 'date downloaded'
            dup = concat_data.duplicated(subset = cols_to_check, keep = 'last')  #keep = last : All duplicates except their last occurrence will be marked as True

            dup_index = dup[dup].index
            concat_data = concat_data.drop(dup_index, axis=0)
            
            # replace concat csv with new data - use concat_master_name to replace

            try:
                concat_data.to_csv(concat_master_name, index = False)
                message = message + "\n Saved in " + concat_master_name
            except FileNotFoundError:
                message = message + "\n No such file or directory: " + concat_master_name
                success = False
            except IOError:
                message = message + "\n IOError: Permission denied for path " + concat_master_name
                success = False
            except AttributeError:
                message = message + "\n Attribute error - no data frame to save"
                success = False
        
    return message, success

def branch_merge(path, csv_path, concat_csv_path, branch_combine_csv_path, project_to_process):
    '''
    Merges parent child and branch project forms into 1 csv file for each project in use (in_use = T)
    '''
    # console message
    print("Running branch_merge... ")
    
    # make folder to store concats in
    try:
        os.mkdir(branch_combine_csv_path)
    except FileExistsError:
        pass
    
    error_msg = 'Errors: '
    error_count = 0
    warn_msg = 'Warnings: '
    warn_count = 0

    # create dict of projects and checks RDSF connection 
    if(path == '\\\\rdsfcifs.acrc.bris.ac.uk\\OH_DART_one\\epicollect_api_data'):
        try:
            project_info_raw = pd.read_csv(path + os.sep + "project_info.csv") 
        except (PermissionError, OSError, FileNotFoundError) as e:
            print("Can't connect to RDSF, can't open project_info.csv")
            print(str(e) + "\nWaiting 1 hour......")
            time.sleep(3600)
            
            # 2nd try an hour later
            try:
                project_info_raw = pd.read_csv(path + os.sep + "project_info.csv") 
            except (PermissionError, OSError, FileNotFoundError) as e:
                print("Can't connect to RDSF, can't open project_info.csv, " + str(e))
                print("\nExiting...")
                send_email_alert("Error connecting to RDSF", msg)
                exit()  
            
    if(path != '\\\\rdsfcifs.acrc.bris.ac.uk\\OH_DART_one\\epicollect_api_data'):
        try:
            project_info_raw = pd.read_csv(path + os.sep + "project_info.csv") 
        except (PermissionError, OSError, FileNotFoundError) as e:
            msg = "Can't find project_info.csv which contains project names and authentication details."
            msg = msg + "\n Trying to access " + path
            msg = msg + "\n Getting error" + str(e)
            print(msg)
            print("\nExiting...")
            exit()   
    
    # replace nones with nan
    project_info_raw = project_info_raw.replace({pd.np.nan: None})
    
    # picks projects in use only
    project_in_use = project_info_raw[project_info_raw.in_use == "T"]
    #project_to_process = "farm-samples" # testing only
    
    # remove un needed projects
    if project_to_process != "all":
        project_in_use = project_info_raw[project_info_raw.project_slug == project_to_process]                    
    
    # gets a list of the porjects to merge                
    unique_projects = project_in_use.project_slug.unique()
    
    # for each project, open all forms and put their data into an array of dfs
    for p in unique_projects:
         print("Merging forms in " + p)
         in_this_project = project_in_use[project_in_use.project_slug == p]
         
         # if only one form, we can't merge, leave
         if len(in_this_project) == 1:
             msg = "Only one form in project " + p + ", skipping"
             warn_msg = warn_msg + "\n" + msg
             warn_count = warn_count + 1
             print(msg)
        # else, we will create an array of dfs
         else:
             form_type = list() # to note the type of form (parent, child, branch, error)
             df_list = list() # an array to store the dfs
             form_name_list = list() # a aray of the names of each form
             
             #file_list = ["parent", "child", "branch"] # for testing only
             
             # go through each form in the project 
             for index, row in in_this_project.iterrows():
             #for i in file_list: # for testing only
                #print(i) # for testing only
                #file_name = concat_csv_path + "_test" + os.sep + i + ".csv" # for testing only
                #print(file_name) # for testing only
                
                file_name = concat_csv_path + os.sep + row['project_slug'] + "_" + row['form_slug'] + "_" + row['branch_name'] + ".csv" # full file path to read in file
                form_name_list = row['project_slug'] + "_" + row['form_slug'] + "_" + row['branch_name'] + ".csv" # full form name
               
                #try and open file
                try:
                    df = pd.read_csv(file_name) 
                    
                # if the file can't be found, skip it    
                except (PermissionError, OSError, FileNotFoundError) as e:
                    msg = "Can't open " + file_name + ", getting error" + str(e)
                    print(msg)
                    print("Skipping this file...")
                    error_msg = error_msg + "\n" + msg + " \nSkipped file"
                    error_count = error_count + 1
                    break
                    
                #add df to array of dfs
                df_list.append(df)
                col_list = df.columns.values.tolist()
                
                # determine the form type by looking at the col names of ec5 cols
                if("ec5_uuid" in col_list) == True:
                    #print("could be parent or child")
                    if("ec5_parent_uuid" in col_list) == True:
                        #print("is a child")
                        form_type.append("child")
                    else:
                        #print("is a parent")
                        form_type.append("parent")
                else:
                    #print("must be a branch")
                    if("ec5_branch_owner_uuid" in col_list) == True: 
                        #print("is definetly a branch")
                        form_type.append("branch")
                    else:
                        print("no idea what this is!")
                        form_type.append("error")
                        
             # after for loop to determine which form is which type, now we merge
            
             counter = len(form_type) # number of forms we have in the project which can be merged
             merged_df = []
             merge_counter = 0
             print(form_type)
             for i in range(0,counter-1):
                 #print(i)
                
                 if merge_counter == 0:
                     #print("no previous merges")
                     previous_merge = df_list[i] # occurs if there are no previous successful merges, our previous merge is just our next/first df
                 
                #merge situations - use the form type list to look a tthe megre situations and merge accordingly
                 
                 # parent and child
                 if form_type[i] == "parent" and form_type[i+1] == "child":
                     try:
                         merged_df = pd.merge(previous_merge, df_list[i+1], left_on='ec5_uuid',right_on='ec5_parent_uuid',how='outer',suffixes=('_parent','_child'))
                     except KeyError:
                         merged_df = pd.merge(previous_merge, df_list[i+1], left_on='ec5_uuid_parent',right_on='ec5_parent_uuid',how='outer',suffixes=('_parent','_child')) #if processed before, the cols will have the suffix on them
                         
                # parent and branch
                 if form_type[i] == "parent" and form_type[i+1] == "branch":
                      try:
                          merged_df = pd.merge(previous_merge, df_list[i+1], left_on='ec5_uuid',right_on='ec5_branch_owner_uuid',how='outer',suffixes=('_parent','_branch'))
                      except KeyError:
                          try: 
                              merged_df = pd.merge(previous_merge, df_list[i+1], left_on='ec5_uuid_parent',right_on='ec5_branch_owner_uuid',how='outer',suffixes=('_parent','_branch'))    
                          except KeyError:
                              error_msg = error_msg + "\n Error merging data in form " +  form_name_list[i] + " and " + form_name_list[i+1]
                              error_count = error_count + 1
                              
                # child and branch
                 if form_type[i] == "child" and form_type[i+1] == "branch":
                     try:
                         merged_df = pd.merge(previous_merge, df_list[i+1], left_on='ec5_uuid',right_on='ec5_branch_owner_uuid',how='outer',suffixes=('_child','_branch'))
                     except KeyError:
                         try: 
                             merged_df = pd.merge(previous_merge, df_list[i+1], left_on='ec5_uuid_child',right_on='ec5_branch_owner_uuid',how='outer',suffixes=('_child','_branch'))    
                         except KeyError:
                             error_msg = error_msg + "\n Error merging data in form " +  form_name_list[i] + " and " + form_name_list[i+1]
                             error_count = error_count + 1
                    
                 if form_type[i] == "branch" and form_type[i+1] == "child":
                     try: 
                         merged_df = pd.merge(previous_merge, df_list[i+1], left_on='ec5_branch_owner_uuid',right_on='ec5_parent_uuid',how='outer',suffixes=('_branch','_child'))#ec5_parent_uuid
                     except KeyError:
                         try:
                             merged_df = pd.merge(previous_merge, df_list[i+1], left_on='ec5_branch_owner_uuid_branch',right_on='ec5_parent_uuid',how='outer',suffixes=('_branch','_child'))
                         except KeyError:
                             error_msg = error_msg + "\n Error merging data in form " +  form_name_list[i] + " and " + form_name_list[i+1]
                             error_count = error_count + 1
                             
                    
                 #non-merge situations
                 if form_type[i] == "child" and form_type[i+1] == "child":
                     reason ="2 children, can't merge"            
                
                 if form_type[i] == "parent" and form_type[i+1] == "parent":
                     reason ="2 parents, can't merge"             
                
                 if form_type[i] == "branch" and form_type[i+1] == "branch":
                     reason = "2 branches, can't merge"
                     
                 if form_type[i] == "branch" and form_type[i+1] == "parent":
                     reason = "Branch form appears before parent form"
                 
                 if form_type[i] == "child" and form_type[i+1] == "parent":
                     reason = "Child form appears before parent form"
                       
                 if form_type[i] == "error":
                     reason = "Unknown form type, can't merge"
                    
                 # if not 0, merge was successful         
                 if len(merged_df) != 0:
                     merge_counter = merge_counter + 1
                     # update the previous_merge
                     previous_merge = merged_df
                 else:
                     print("Merge not a success")
                     error_msg = error_msg + "\n Error merging data in form " +  form_name_list[i] + " and " + form_name_list[i+1] + "\n" + reason 
                     # don't update previous merge or add to merge_counter
                     
                     
             #print("end of inner loop to merge")
             print("Number of merges = " + str(merge_counter))
             if merge_counter > 0:
                 #print("saving merge")
                 folder = branch_combine_csv_path
                 full_path = folder + os.sep + p + ".csv"
                 merged_df.to_csv(full_path, index = False)
                 print("Saved " + p + ".csv ")
             #else:
                 #print("no merges, not saving")

    print("End of all projects - merges saved in " + branch_combine_csv_path)
    print(str(error_count))
    print(error_msg)
    
    return error_msg, error_count, warn_msg, warn_count