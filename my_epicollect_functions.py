# -*- coding: utf-8 -*-
"""
Created on Mon Sep 23 20:00:07 2019

@author: lv13916
"""


def exit(status=0, message=None):
    """
    Display error messages to user
    """
    if message:
        print(message)
        print(sys.stderr)
    sys.exit(status)
    

def convert_to_datetime(dt):
    """ 
    Takes epicollect string of date time and converts to a datetime.datetime object for calcs
    """
    if (type(dt) == str):
        date_pattern = re.compile("^\d{4}-\d{2}-\d{2}")
        time_pattern = re.compile("\d{2}:\d{2}:\d{2}")
        formatted_str = date_pattern.search(dt).group(0) + ' ' + time_pattern.search(dt).group(0)
        dt = datetime.strptime(formatted_str, "%Y-%m-%d %H:%M:%S")
        return dt
    else:
        print("Must be string")
        return None

def convert_to_excel_date(dt):
    """ 
    Takes epicollect string of date time and converts to excel format date
    """
    if (type(dt) == str):
        date_pattern = re.compile("^\d{4}-\d{2}-\d{2}")
        time_pattern = re.compile("\d{2}:\d{2}:\d{2}")
        date_part = date_pattern.search(dt).group(0)
        time_part = time_pattern.search(dt).group(0)
        split1 = date_part.split('-')
        dt = split1[2] + '/' + split1[1] + '/' + split1[0]
        return dt
    else:
        print("Must be string")
        return None
           
def convert_to_excel_time(dt):
    import re
    """ 
    Takes epicollect string of date time and converts to excel format time
    """
    if (type(dt) == str):
        time_pattern = re.compile("\d{2}:\d{2}:\d{2}")
        dt = time_pattern.search(dt).group(0)
        return dt
    else:
        print("Must be string")
        return None    

#detect image files by str contains image file extension
def detectImage(value):
    pattern = re.compile(".gif$|.ico$|.jpeg$|.jpg$|.svg$|.tiff$|.tif$|.webp$") 
    if value != None and type(value) == str:
        if type(pattern.search(value)) == re.Match:
            return True
        else:
            return False
    else:
        return False

#detect audio files by str contains image file extension
def detectAudioVideo(value):
    pattern = re.compile("aac$|.mid$|.midi$|.ogg$|.wav$|.weba$|.3gp$|.3g2$|.mp4$|.avi$|.mpeg$|.mpg$|.ogv$|.webm$|.3gp$|.3g2$")
    if value != None and type(value) == str:
        if type(pattern.search(value)) == re.Match:
            return True
        else:
            return False
    else:
        return False


# make folder structure
def createFolderStructure(path, col_name, created_by, folder_name):        

    folder_name = path + os.sep + folder_name
    try:
        os.mkdir(folder_name)
        top_folder = folder_name
    except FileExistsError:
        top_folder = folder_name
    
    # sub folder = user
    folder_name = top_folder + os.sep + col_name
    try:
        os.mkdir(folder_name)
        sub_folder = folder_name
    except FileExistsError:
        sub_folder = folder_name 
        
    # subsub folder = user
    folder_name = sub_folder + os.sep + created_by
    try:
        os.mkdir(folder_name)
        subsub_folder = folder_name
    except FileExistsError:
        subsub_folder = folder_name 
    #print(subsub_folder)
    return subsub_folder

# download image
def downloadImage(access_token, project_slug, image_path, image_name, col_name, created_by, folder_name):
    time.sleep(2)
    media = pyep_get_media(project_slug, image_name, token=access_token, type = 'photo', format = 'entry_original')
    error_image = pyep_get_media(project_slug, "wrong_image.jpg", token=access_token, type = 'photo', format = 'entry_original')
    
    # check server response
    if (str(media)=='<Response [429]>'):
        print("Waiting for server")
        
        while (str(media)=='<Response [429]>'):
            time.sleep(5)
            media = pyep_get_media(project_slug, image_name, token=access_token, type = 'photo', format = 'entry_original')
            error_image = pyep_get_media(project_slug, "wrong_image.jpg", token=access_token, type = 'photo', format = 'entry_original')
    
    # check server response
    if (str(media)!='<Response [200]>' or str(error_image)!='<Response [200]>'):
        print("Server error: " + str(media) + "  " + str(error_image))
        return False, "not saved"
    
    # check media doesn't match the logo image which is downloaded when the filename is wrong, compare bytes
    if (sum(error_image.content) == sum(media.content)) == True: #if these are the same then the image is the error image
        print('Placeholder image - image not fully uploaded')
        return False, "not saved"
    else: #save image
        output_path = createFolderStructure(image_path, col_name, created_by, folder_name)
        file_name = output_path + os.sep + image_name.split('.')[0] + '.jpg'
        with open(file_name, "wb") as jpg:
            jpg.write(media.content)
            jpg.close()
        print("Saved" + file_name + " in " + output_path)
        return True, file_name

# download audio
def downloadAudio(access_token, project_slug, audio_path, audio_name, col_name, created_by, folder_name):
    time.sleep(2)
    media = pyep_get_media(project_slug, audio_name, token=access_token, type = 'audio', format = 'audio')
    error_image = pyep_get_media(project_slug, "nerhfkb.mp4", token=access_token, type = 'audio', format = 'audio')
   
     # check server response - added July 2020
    if (str(media)=='<Response [429]>'):
        print("Waiting for server")
        
        while (str(media)=='<Response [429]>'):
            time.sleep(5)
            media = pyep_get_media(project_slug, audio_name, token=access_token, type = 'audio', format = 'audio')
            error_image = pyep_get_media(project_slug, "nerhfkb.mp4", token=access_token, type = 'audio', format = 'audio')
    
    # check server response
    if (str(media)!='<Response [200]>' or str(error_image)!='<Response [200]>'):
        print("Server error: " + str(media) + "  " + str(error_image))
        return False, "not saved"
    
    # check media doesn't match the logo image which is downloaded when the filename is wrong, compare bytes
    if (sum(error_image.content) == sum(media.content)) == True: #if these are the same then the image is the error image
        #print(media)
        print('Placeholder audio - audio not fully uploaded')
        return False, "not saved"
    else: #save audio
        output_path = createFolderStructure(audio_path, col_name, created_by, folder_name)
        file_name = output_path + os.sep + audio_name
        with open(file_name, "wb") as audio:
            audio.write(media.content)
            audio.close()
        print("Saved" + file_name + " in " + output_path)
        return True, file_name
    
# download audio
def downloadVideo(access_token, project_slug, video_path, audio_name, col_name, created_by, folder_name):
    time.sleep(2)
    media = pyep_get_media(project_slug, audio_name, token=access_token, type = 'video', format = 'video')
    error_image = pyep_get_media(project_slug, "nerhfkb.mp4", token=access_token, type = 'video', format = 'video')
    
     # check server response
    if (str(media)=='<Response [429]>'):
        print("Waiting for server")
        
        while (str(media)=='<Response [429]>'):
            time.sleep(5)
            media = pyep_get_media(project_slug, audio_name, token=access_token, type = 'video', format = 'video')
            error_image = pyep_get_media(project_slug, "nerhfkb.mp4", token=access_token, type = 'video', format = 'video')
    
    # check server response
    if (str(media)!='<Response [200]>' or str(error_image)!='<Response [200]>'):
        print("Server error: " + str(media) + "  " + str(error_image))
        return False, "not saved"
    
    # check media doesn't match the logo image which is downloaded when the filename is wrong, compare bytes
    if (sum(error_image.content) == sum(media.content)) == True: #if these are the same then the image is the error image
        #print(media)
        return False, "not saved"
    else: #save video
        output_path = createFolderStructure(video_path, col_name, created_by, folder_name)
        file_name = output_path + os.sep + audio_name
        with open(file_name, "wb") as video:
            video.write(media.content)
            video.close()
        print("Saved" + file_name + " in " + output_path)
        return True, file_name

def convert_dt_to_str(dt):
    """ 
    Takes the token request time and converts to a string which can be used in a folder name
    """
    if (type(dt) == str):
        y = dt[2:4]
        m = dt[5:7]
        d = dt[8:10]
        dt = d + "-" + m + "-" + y + "_" + dt[11:16].replace(":", "")
        return dt
    else:
        print("Must be string")
        return None    


def check_exists(project_name, project_slug):   
    # checking project exisits
    result = pyep_search_project(project_name)
    try:
        result['data'][0]['project']['slug']
        if result['data'][0]['project']['slug'] == project_slug:
            return True
        else:
            message = "Can't find project, incorrect name: " + project_name
        return message 
    except IndexError:
        message = "Can't find project, incorrect name: " + project_name
        return message 
    

def get_token(client_id, client_secret):
    token = pyep_request_token(client_id, client_secret)
    try: 
        access_token = token['access_token']
        request_time = token['request_time']
        return True, token
    except KeyError:
        token_error_log = "Error getting token: " + token['errors'][0].get('title')
        return False, token_error_log

def get_entries(project_slug, folder_name, form_ref, branch_ref, access_token, last_collection):
    """ 
    Calls the Epicollect API to get the data from the project from the last collection date to present.
    Puts data into a panda df. Counts missing values (not used currently).
    Returns a pandas df or a str with an error message. 
    """
    # changed to uplaoded at July 2020
    entries = pyep_get_entries(project_slug, access_token, form_ref = form_ref, branch_ref = branch_ref, filter_by = "uploaded_at", filter_from = last_collection)
    
    #getting page 1
    try: 
        entry_list_page1 = entries['data']['entries']
    except KeyError:
        error_message = "Entries not found in project " + folder_name
        return False, error_message
    try:
        key_list = entry_list_page1[0].keys()
    except IndexError:
        error_message = "No new entries since " + str(last_collection) + " in project " + folder_name
        return False, error_message
    
    # finding out how many pages
    per_page = entries['meta']['per_page']
    total_entries = entries['meta']['total']
    total_pages = math.floor(total_entries/per_page+1)
    
    all_entries_list = list(list(entry_list_page1))
    # get all entries, not just first page
    for page in range(1, total_pages):
        time.sleep(2) # sleep before each page because only 60 requests a minute
        # start at page 2 because page 1 is already fetched
        # changed to uplaoded at July 2020
        entries_page = pyep_get_entries(project_slug, access_token, form_ref = form_ref, branch_ref = branch_ref, filter_by = "uploaded_at", filter_from = last_collection, page=page+1)
        
        entry_list_nextpage = entries_page['data']['entries']
        
        for i in range(0, len(entry_list_nextpage)):
            #print(i)
            all_entries_list.append(entry_list_nextpage[i])
            
    if (len(all_entries_list) != total_entries):
        print("Error, wrong number of entries fetched")
        
    df = pd.DataFrame(columns=key_list)
    i = 0
    missing_list = list() # currently unused
    for entry in all_entries_list:
        value_list = list()
        for key in key_list:
            value = entry.get(key)
            
            if type(value) == str and len(value) == 0:
                missing_list.append([key, i])
                value = None
                
            value_list.append(value)
            
        df.loc[i] = value_list
        i = i + 1
    
    if (len(df) == total_entries):
        print("Success, right number of entries fetched")
    else:
        print("Error, wrong number of entries fetched")
        
    return True, df
    
def get_images(df, project_slug, folder_name, image_path, access_token):
    """
    Finds and downloads images from EpiCollect.
    First identifies cells in the dataframe which have images.
    Images must be: 'gif', 'ico', 'jpeg', 'jpg', 'svg', 'tiff', 'tif', 'webp'
    Returns bool (success or not) and a user message to save how many images have saved.
    """
    result = df.applymap(detectImage)
    image_locs = result.iloc[np.unique(np.flatnonzero((result==True).values)//result.shape[1]), np.unique(np.flatnonzero((result==True).values)%result.shape[1]) ]
    
    # no image in project
    if image_locs.empty == True:
        user_message = "No images to save"
        return False, user_message
    
    image_counter = 0
    message =  ''
    #save those images in an appropriate place
    for row in range(0,df.shape[0]):
        #print(row)
        try:
            list(image_locs.index).index(row)
            for col_name in image_locs.columns:
                image_name = df[col_name][row]
                if image_name == None:
                    #print("None")
                    image_name = image_name # placeholder
                else:
                    success = downloadImage(access_token, project_slug, image_path, image_name, col_name, df['created_by'][row].split('@')[0], folder_name)
                    if success[0] == True:
                        print("Image saved at " + success[1])
                        image_counter += 1
                        df[col_name][row] = success[1] #change image name to path of image saved
                    else:
                        message = message + "\nUnable to save image: " + image_name + " in row " + str(row)
        except ValueError: # due to no images in the row
            image_counter = image_counter #place holder for the exception

    # have we saved the expected amount of images?
    if (image_locs.sum().sum() == image_counter):
        message = message + "\nSaved " + str(image_counter) +" images"
        return True, message, df
    else:
        report = "\nNot all images saved correctly, was expecting to save " + str(image_locs.sum().sum()) + " images, saved " + str(image_counter)
        message = message + report
        return False, message

def get_audio_video(df, project_slug, folder_name, audio_path, video_path, access_token):
    """
    Finds and downloads audio and video from EpiCollect.
    First identifies cells in the dataframe which have images.
    Audio must be aac, mi, midi, ogg, wav, weba, 3gp3, g2 or mp4
    If mp4, could also be a video.
    Tries audio download first, if that fails, tries video
    Returns bool (success or not) and a user message to save how many images have saved.
    """
    result = df.applymap(detectAudioVideo) #looks for audio and video endings
    audio_locs = result.iloc[np.unique(np.flatnonzero((result==True).values)//result.shape[1]), np.unique(np.flatnonzero((result==True).values)%result.shape[1])]
    
    # no audio in project
    if audio_locs.empty == True:
        user_message = "No audio or video to save"
        return False, user_message
    
    audio_counter = 0
    video_counter = 0
    message =  ''
    #save those images in an appropriate place
    for row in range(0,df.shape[0]):
        #print(row)
        try:
            list(audio_locs.index).index(row)
            for col_name in audio_locs.columns:
                audio_name = df[col_name][row]
                if (audio_name == None):
                    audio_name = audio_name # placeholder
                    #print("None")
                else:
                    success = downloadAudio(access_token, project_slug, audio_path, audio_name, col_name, df['created_by'][row].split('@')[0], folder_name)
                    if success[0] == True:
                        print("Audio saved at " + success[1])
                        audio_counter += 1
                        df[col_name][row] = success[1] #change image name to path of image saved
                    else:
                       # print("Unable to save - could be video?")
                        success = downloadVideo(access_token, project_slug, video_path, audio_name, col_name, df['created_by'][row].split('@')[0], folder_name)
                        if success[0] == True:
                            print("Video saved at " + success[1])
                            video_counter += 1
                            df[col_name][row] = success[1] #change image name to path of image saved
                        else:
                            print("Unable to save as either audio or video")
                            message = message + "\nUnable to save audio/video: " + audio_path + " in row " + str(row)
        except ValueError: # due to no images in the row
            audio_counter = audio_counter #place holder for the exception

            
    # have we saved the expected amount of images?
    if (audio_locs.sum().sum() == audio_counter + video_counter):
        message = message + "\nSaved " + str(audio_counter) +" audio files" + " and " + str(video_counter) +" video files"
        return True, message, df
    else:
        report = "\nNot all audio/video saved correctly, was expecting to save " + str(audio_locs.sum().sum()) + " files, saved " + str(audio_counter + video_counter)
        message = message + report
        return False, message
    
    
def save_data(df, form_name, csv_path, request_time):
    # Saving data to csv - image names have been changed to full file paths
    form_folder = csv_path + os.sep + form_name #make a folder for a the form
    num_rows = df.shape[0]
    try:
        os.mkdir(form_folder)
        folder_path = form_folder
    except FileExistsError:
        folder_path = form_folder    
    dt_str = str(request_time.isoformat().split('+')[0][0:-3] + 'Z').replace(':','.') #make a folder for the request  
    # try to save as csv   
    try:
        df.to_csv(folder_path + os.sep + dt_str + '.csv', index = False)
        message = str(num_rows) + " rows of data saved in " + folder_path + os.sep + dt_str + '.csv'
        success = True
        print(message)
    except FileNotFoundError:
        message = "No such file or directory: " + folder_path + os.sep
        success = False
    except IOError:
        message = "IOError: Permission denied for path " + folder_path + os.sep
        success = False
    except AttributeError:
        message = "Attribute error - no data frame to save"
        success = False
    return success, message



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

    
def harvest_epicollect(project_info_dict, csv_path, image_path, audio_path, video_path):
    """
    Main function which gets all epicollect data from date specified from the form specified 
    """
    #testing only
    #last_collection = '2010-11-10T16:04:40.637Z'
    user_message = ''
    #auth
    client_id = int(project_info_dict.get('client_id'))
    client_secret = str(project_info_dict.get('client_secret'))
    
    #names
    project_name = str(project_info_dict.get('project_name'))
    project_slug = str(project_info_dict.get('project_slug'))
    folder_name = str(project_info_dict.get('project_slug') + "_" + project_info_dict.get('form_slug') + "_" + project_info_dict.get('branch_name'))
    
    #references
    form_ref = str(project_info_dict.get('form_ref'))
    branch_ref = str(project_info_dict.get('branch_ref'))
    if branch_ref == "None":
        branch_ref = None
    #last collection date
    last_collection = str(project_info_dict.get('last_collection'))
    
    #testing only
    #last_collection = '2010-11-10T16:04:40.637Z'

    user_message = "Project: " + project_slug + "    Form: " + folder_name + " \nLast collection date: " + last_collection

    check_exists_output = check_exists(project_name, project_slug)
    if (type(check_exists_output) == "str"):
        user_message = user_message + "\n" + check_exists_output
        return False, user_message

    # get token 
    get_token_output = get_token(client_id, client_secret)
    if get_token_output[0] == False:
        user_message = user_message + "\n" + get_token_output[1]          
    access_token = get_token_output[1]['access_token']
    request_time = get_token_output[1]['request_time']
    
    # get entries
    get_entries_output = get_entries(project_slug, folder_name, form_ref, branch_ref, access_token, last_collection)
    if (get_entries_output[0] == False) :
        user_message = user_message + "\n" + get_entries_output[1]
        return False, user_message
        
    df = get_entries_output[1]
    
    #get images
    get_images_output = get_images(df, project_slug, folder_name, image_path, access_token)

    if (get_images_output[0] == False) and (get_images_output[1] != 'No images to save'):
        success =  False  # don't return if there is an error - data can still be saved in csv even if images failed, use if else instead
    if (get_images_output[0] == False) and (get_images_output[1] == 'No images to save'):
        success = True #no images is not an error
    if get_images_output[0] == True:
        df = get_images_output[2] # save df as it hs been updated with the full file paths
        success = True

    user_message = user_message + "\n" + get_images_output[1]
    
    #get audio
    get_audio_output = get_audio_video(df, project_slug, folder_name, audio_path, video_path, access_token)
    if (get_audio_output[0] == False) and (get_audio_output[1] != 'No audio or video to save'):
        success =  False  # don't return if there is an error - data can still be saved in csv even if images failed, use if else instead
    if (get_audio_output[0] == False) and (get_audio_output[1] == 'No audio or video to save'):
        success = True #no images is not an error
    if get_audio_output[0] == True:
        df = get_audio_output[2] # save df as it hs been updated with the full file paths
        success = True

    user_message = user_message + "\n" + get_audio_output[1]

    # save data
    save_data_output = save_data(df, folder_name, csv_path, request_time)
    if save_data_output[0] == True:
        user_message = user_message + "\n" + save_data_output[1]
        success = True
    else:
        user_message = user_message + "\n" + save_data_output[1]
        success = False
        
    # complete    
    return success, user_message, folder_name, request_time

        

    
    




