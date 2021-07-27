import pandas as pd
import numpy as np
import datetime
import time,  re
import snowflake.connector
from snowflake.sqlalchemy import URL
import os,shutil
from urllib.request import urlretrieve
from bs4 import BeautifulSoup
from selenium import webdriver
#from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from os import path
from config import *
import ds_commons
from Encryptor import *
import threading
from concurrent.futures import ThreadPoolExecutor

def createNewRNMThread(pname,pid, prjFolder):

    rnm_thread = threading.Thread(target=rnmFiles, args=(pname,pid, prjFolder))
    rnm_thread.start()


def rnmFiles(pname,pid, prjFolder):
        time.sleep(10)
        con1 = snowflake.connector.connect(
          user=vUser,
          password=vPwd,
          account=vAccount,
          paramstyle='qmark',
          warehouse= vWarehouse,
          database=vDatabase)
		  
       #completing move 			
        ds_commons.logDSAudit('scenecronized',pname.replace("'","''"),pname.replace("'","''"),'Python','scenechronize Scripts Download_FNOR','L',vLOG_DATE,'Project file Rename with OLD text Started',v_batch_no) 			
        # Prefix existing files in Shared Drive with "OLD_" and delete them later
        pattern=r"FNOR_[0-9]+_" + str(pid)		
        files = [i for i in os.listdir(vOneDrivePath) if ( re.findall(pattern, i))]

        for file in files:
           try:
              print("Renaming to OneDrive" + vOneDrivePath  + file)		   
              os.rename( vOneDrivePath + file, vOneDrivePath + "OLD_" + file)
           except Exception as e:
              print("File already exists")
           #os.remove( vOneDrivePath + file)		


        if vCopySharedDrive=='Y':
           pattern=r"ENC_FNOR_[0-9]+_" + str(pid)				   
           files = [i for i in os.listdir(vSharedDrivePath) if ( re.findall(pattern, i))]

           for file in files:
              try:	
                 print("Renaming to shared drive " +vSharedDrivePath +  file)			  
                 os.rename( vSharedDrivePath + file, vSharedDrivePath + "OLD_" + file)
              except Exception as e:
                 print("File already exists")			  
              #os.remove( vOneDrivePath + file)	


        ds_commons.logDSAudit('scenecronized',pname.replace("'","''"),pname.replace("'","''"),'Python','scenechronize Scripts Download_FNOR','L',vLOG_DATE,'Project file Rename with OLD text Completed',v_batch_no) 			
		   


        ds_commons.logDSAudit('scenecronized',pname.replace("'","''"),pname.replace("'","''"),'Python','scenechronize Scripts Download_FNOR','L',vLOG_DATE,'Project file Rename Started',v_batch_no) 		
	

        files = [i for i in os.listdir(prjFolder) if (not i.startswith("FNOR_")) and  (not i.startswith("parsed_")) and path.isfile(path.join(prjFolder, i))]
		
        for file in files:
            print(file) 
            res = re.findall(r"\s\((\d)\).", file) 
            filename, extension = os.path.splitext(file)  
                
            if res and re.split(" \(\d\)",filename)[0] in file_names:
                new_filename="FNOR_" + str(res[0])+'_'+pid+'_'+re.sub('\s\((\d)\)', '', file)
            else:
                new_filename="FNOR_0_"+pid+'_'+filename +extension
            
            print("old filename " + file)	
            print("new filename " + prjFolder + new_filename) 			
            vLOG_DATE_START=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            os.rename( prjFolder + file, prjFolder + new_filename)
            if vCopySharedDrive=='Y':			
               encScript(prjFolder , new_filename) 

			
            shutil.move(prjFolder + new_filename, vOneDrivePath)			
            time.sleep(2)			
            #KNB			
            v_sql="insert into audm.ds.\"90_SCRIPT_SCNZE_SCRAPPING_LOG\"(log_type,project_id,file_id,scrapping_date,is_deleted,batch_no,FILENAME,ORG_FILENAME) values ('Download'," + str(pid) + ",NULL,current_timestamp()::timestamp_ntz,'N','" + str(v_batch_no) +"','"  + new_filename.replace("'","''") + "','" + file.replace("'","''") + "')"
			
            con1.cursor().execute(v_sql)
			
         
        ds_commons.logDSAudit('scenecronized',pname.replace("'","''"),pname.replace("'","''"),'Python','scenechronize Scripts Download_FNOR','L',vLOG_DATE,'Project file Rename Completed',v_batch_no) 					
        ds_commons.logDSAudit('scenecronized',pname.replace("'","''"),pname.replace("'","''"),'Python','scenechronize Scripts Download_FNOR','L',vLOG_DATE,'Project OLD file Delete Started',v_batch_no) 			
        # Delete old files from OneDrive
        pattern=r"OLD_FNOR_[0-9]+_" + str(pid)		
        files = [i for i in os.listdir(vOneDrivePath) if ( re.findall(pattern, i))]

        for file in files:
           try:		
              #print(" Remove " + vOneDrivePath + file)		
              #os.remove( vOneDrivePath + file)	
              print(file + " moving to " + "C:\\ScriptIntel\\Onedrive_old_files\\") 		
              shutil.move(vOneDrivePath + file, "C:\\ScriptIntel\\Onedrive_old_files\\")              
           except Exception as e:
              continue
			  
        if vCopySharedDrive=='Y':
           # Delete old files from OneDrive

           pattern=r"OLD_ENC_FNOR_[0-9]+_" + str(pid)		
           files = [i for i in os.listdir(vSharedDrivePath) if ( re.findall(pattern, i))]

           for file in files:
              try: 		   
                 #print(" Remove " + vSharedDrivePath + file)			   
                 #os.remove(vSharedDrivePath + file)
                 print(file + " moving to " + "C:\\ScriptIntel\\H_Drive_old_files\\") 		
                 shutil.move(vSharedDrivePath + file, "C:\\ScriptIntel\\H_Drive_old_files\\")                 
              except Exception as e:
                 continue

        ds_commons.logDSAudit('scenecronized',pname.replace("'","''"),pname.replace("'","''"),'Python','scenechronize Scripts Download_FNOR','L',vLOG_DATE,'Project OLD file Delete Completed',v_batch_no) 			   
        v_sql="DELETE FROM AUDM.DS.\"90_SCRIPT_SCNZE_PRJCT_QUEUE\" WHERE Project_id="+str(pid)		
        print(v_sql)		
        con1.cursor().execute(v_sql)
        shutil.rmtree(prjFolder)		
        con1.close()        

def encScript(sourcefolder,filename):

   encryptor=Encryptor()

   #mykey=encryptor.key_create()

   #encryptor.key_write(mykey, 'mykey.key')

   loaded_key=encryptor.key_load('mykey.key')
   encryptor.file_encrypt(loaded_key, sourcefolder+filename, vSharedDrivePath + 'ENC_' + filename)

   

def retString(pString):
   try:
      print("in function " + pString) 
      re_pattern = r'title='
      m = re.search(re_pattern, pString)
      t=(pString[m.end():])

      matches=re.findall(r'\"(.+?)\"',t)
      print(matches)
      return matches[0]
   except Exception as e:
      return None
   

if __name__=='__main__':   
   pool = ThreadPoolExecutor(max_workers=1)   
   con = snowflake.connector.connect(
          user=vUser,
          password=vPwd,
          account=vAccount,
          paramstyle='qmark',
          warehouse= vWarehouse,
          database=vDatabase)

   with snowflake.connector.connect(
                user=vUser,
                password=vPwd,
                account=vAccount
                ) as cnx:
      query='''
             SELECT p.project,p.project_id,pq.batch_no
               FROM AUDM.ds."90_SCRIPT_SCNZE_PRJCT" p
         INNER JOIN (SELECT project_id,max(batch_no) batch_no
                       FROM AUDM.ds."90_SCRIPT_SCNZE_PRJCT_QUEUE"
                   GROUP BY project_id)  pq ON p.project_id=pq.project_id
				   order by 2
          '''
	  
  
      ids=cnx.cursor().execute(query).fetchall()
      ids=pd.DataFrame(ids,columns=["Project","Project_id","Batch_No"])
      #ids=ids[0:1]


      #download_path = "C:\\MyMachine\\work\\Scenecronized\\Files\\"
   #vOneDrivePath = "C:\\Users\\NKuchibhotla\\Links\\Sony\\Ganji, Mohamad - Scripts\\"

   chrome_options = webdriver.ChromeOptions()

   prefs = {"plugins.always_open_pdf_externally": True,

           "download.default_directory": vDownloadPath
          }
   chrome_options.add_experimental_option('prefs',prefs)
   driver = webdriver.Chrome("C:\\Program Files\\Python37\\Scripts\\chromedriver.exe", options=chrome_options)
   #driver = webdriver.Chrome("C:/Users/mganji/Downloads/chromedriver", options=chrome_options)
   #driver = webdriver.Chrome("C:\\MyMachine\\work\\Scenecronized\\chromedriver_win32\\chromedriver", options=chrome_options)
   #driver = webdriver.Chrome(ChromeDriverManager().install() , options=chrome_options)

   driver.get("https://www.scenechronize.com/")
   action = ActionChains(driver)
   driver.maximize_window()
   content = driver.page_source
   soup = BeautifulSoup(content)
   wait = WebDriverWait(driver, 120)
   myElem = wait.until(lambda driver: driver.find_element_by_id("gwt-uid-3"))
   print("Page is ready!")
   driver.find_element_by_id("gwt-uid-3").send_keys(vSchUser)
   driver.find_element_by_xpath('//button[@class="btn right"]').click()
   myElem = wait.until(lambda driver: driver.find_element_by_id("login-password-input"))
   print("Page is ready1!")
   driver.find_element_by_id("login-password-input").send_keys(vSchPwd)
   driver.find_element_by_id("login-password-continue").click()
   myElem = wait.until(lambda driver: driver.find_element_by_id("login-mfa"))
   print("Page is ready2!")

   myElem = wait.until(lambda driver: driver.find_element_by_id("scenechronize"))
   print("Page is ready3!")
   time.sleep(10)
   count_for_pdfList  = 0
   content = driver.page_source
   soup = BeautifulSoup(content, features="html.parser")
   text1= 'Studio Documents'
   text2= 'SPT US'
   text3= 'Production'
   text4= 'Script'
   links = []
   answer = []
   new_files = []
   new_set_links = []
   date_time = []
   search_button_element = "/html/body/div[2]/div[2]/div/div[5]/div/div/div/div[3]/div/div[3]/div/div[2]/div/div[3]/span/div/input"
   #search_button_element = "/html/body/div[2]/div[2]/div/div[5]/div/div[1]/div/div[3]/div/div[3]/div/div[2]/div/div[3]/span/div/input"
   studio = "//div[contains(text(),'STUDIO')]"
   home = "//div[contains(text(),'HOME')]"
   clear_search = "/html/body/div[2]/div[2]/div/div[5]/div/div[1]/div/div[3]/div/div[3]/div/div[2]/div/div[3]/span/div/i"
   symbol = "/html/body/div[2]/div[2]/div/div[5]/div/div/div/div[3]/div/div[3]/div/div[2]/div/div[5]/div/div[5]/div/div[3]/div/div/div/div[1]/div[2]/i"
   searched_show = "/html/body/div[2]/div[2]/div/div[5]/div/div[1]/div/div[3]/div/div[3]/div/div[2]/div/div[5]/div/div[5]/div/div[3]/div/div/div/div[1]/div[3]/div/span"
   show_after_search = "/html/body/div[2]/div[2]/div/div[5]/div/div/div/div[3]/div/div[3]/div/div[2]/div/div[5]/div/div[5]/div/div[3]/div/div/div/div[1]/div[3]/div/span"
   show_already_selected = "/html/body/div[2]/div[2]/div/div[2]/div/div[2]/div/div"
   download_button = "//cr-icon-button[@title='Download']"
   all_df_list = []
   time.sleep(5)  #Added by Tarun
   search_button = driver.find_element_by_xpath(search_button_element)



   #proj = ["17th Precinct - Pilot","Outlander 17/18 Season 4", "McCarthy's #2 Pilot"]
   #ids= pd.DataFrame({"Project":proj,"Project_id":[i for i in range(len(proj))]})
       
        
   get_title = []
   get_document_label = []
   get_document_date = []
   get_file_type = []
   get_size = []
   get_added_by = []
   get_date_added = []
   get_document_type = []
   project_name = []
   get_Episodes_name = []
   get_Pages = []
   get_Scenes = []
   result=[]

   failed_links=pd.DataFrame()
   print(ids)
   ids2=pd.DataFrame()
   qwe = 0
   while qwe < 3:
      
      for (show,show_id,batch_no) in [tuple(x) for x in ids.to_numpy()]:
       print(show,show_id,batch_no)
       try:
          vLOG_DATE=date = pd.to_datetime('today')
          v_batch_no=batch_no
          ds_commons.logDSAudit('scenecronized',show.replace("'","''"),show.replace("'","''"),'Python','scenechronize Scripts Download_FNOR','L',vLOG_DATE,'Project file read started',v_batch_no) 					
       	
          result = []
          get_title = []
          all_df_list = []
          get_document_label = []
          get_document_date = []
          get_file_type = []
          get_size = []
          get_added_by = []
          get_date_added = []
          get_document_type = []
          project_name = []
          get_Episodes_name = []
          get_Pages = []
          get_Scenes = []
          answer = []
          new_files = []
          new_set_links = []
          date_time = []
          files=[]
          answer=[]
          #    folders = str(show)
          #    pat = '\W+'
          #    new = ''
          #    new_folders = re.sub(pat, new, str(folders))
          #print("to numpy inside ", ids.to_numpy())
          #print("present show and id",show,show_id,batch_no)
          new_folder=str(show_id)

          #folders = str(show).replace(" ", '').replace("/",'_')
          #pat = '\W+'
          #new = ''
          #new_folders = re.sub(pat, new, str(folders))
          #print('folder....', new_folders)
          if not os.path.exists(vDownloadPath + new_folder):
             os.mkdir(vDownloadPath + new_folder)
          destination = vDownloadPath + new_folder + "\\"
          print(destination)

          print("Current show", show, show_id)
          s_id=str(show_id)

          #new_folders=str(new_folders)

          #print(s_id)
		
	
		   
          driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div[5]/div/div/div/div[3]/div/div[3]/div/div[2]/div/div[3]/span/div/input').send_keys(show)
          time.sleep(5)
          driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div[5]/div/div/div/div[3]/div/div[3]/div/div[2]/div/div[5]/div/div[5]/div/div[3]/div/div/div/div[1]/div[3]/div/span').click()
          time.sleep(10)
    
          studio = "//div[contains(text(),'STUDIO')]"
          element = WebDriverWait(driver,180).until(EC.presence_of_element_located((By.XPATH,studio)))
          element.click()
          time.sleep(10)

          studio_documents = "/html/body/div[2]/div[2]/div/div[5]/div/div[2]/div/div/div[1]/div[1]"
          studio_documents_element = driver.find_element_by_xpath(studio_documents)
          time.sleep(5)
          # spt_us = "/html/body/div[2]/div[2]/div/div[5]/div/div[2]/div/div/div[3]/div/div[2]/div/div[3]/div/div/div/div[1]/div[4]/div/span/a/span"
          # production = "/html/body/div[2]/div[2]/div/div[5]/div/div[2]/div/div/div[3]/div/div[2]/div/div[3]/div/div/div/div[2]/div[4]/div/span/a/span"
          # src = driver.page_source
          # if text2 not in src and text3 not in src and text4 not in src:
          if studio_documents_element.is_enabled():
             print("validating PDF Document when studio document is clickable")
             time.sleep(6)
             element1 = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, studio_documents))).click()
             #print("Studio documents")				
             #time.sleep(6)
             time.sleep(6)
             element2 = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH,
                                                                                        "//span[contains(text(),'SPT US')]"))).click()
             #print("validating PDF Document when SPT US is clickable")																						
             time.sleep(6)
             element3 = WebDriverWait(driver, 30).until(
                 EC.presence_of_element_located((By.XPATH, "//span[@title='Production']"))).click()

			 #un-commented by Nag on 1/14/2021 
             time.sleep(6)
             element4 = WebDriverWait(driver, 30).until(
                 EC.presence_of_element_located((By.XPATH, "//span[contains(text(),'Script')]"))).click()

             time.sleep(8)
          else:
             print("Studio Document not clickable")
             element5 = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH,
                                                                                        "//span[contains(text(),'SPT US')]"))).click()
             element6 = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, "//span[@title='Production']"))).click()
             element7 = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, "//span[contains(text(),'Script')]"))).click()
             time.sleep(8)
             # element4 = WebDriverWait(driver, 120).until(EC.presence_of_element_located((By.XPATH, script))).click()

          #NEW LINES ADDED BY TARUN:
          element = "/html/body/div[2]/div[2]/div/div[5]/div/div[2]/div/div/div[3]/div/div[2]/div/div[2]/div/div[2]/div/span/label"
          driver.find_element_by_xpath(element).click()
          time.sleep(3)

          qq = driver.find_element_by_id("gwt-uid-6").is_selected()
          time.sleep(3)
          
          if qq:
            element = "/html/body/div[2]/div[2]/div/div[5]/div/div[2]/div/div/div[3]/div/div[3]/div/span"
            num = driver.find_element_by_xpath(element).text
            print(num)
            temp6=""
            for jjj in num:
               if jjj.isnumeric():
                  temp6+=jjj
            
            temp6=int(temp6)
            temp6+=2
            print('Temp6=',temp6)
            time.sleep(5)
          else:
             temp6=2
             print(temp6)
             
          checkbox_inthe_page = driver.find_element_by_xpath("/html/body/div[2]/div[2]/div/div[5]/div/div[2]/div/div/div[3]/div/div[2]/div/div[3]/div/div/div/div[1]/div[4]/div/span/a/span")
          # WebDriverWait(driver,5).until(EC.element_to_be_clickable(By.XPATH("/html/body/div[2]/div[2]/div/div[5]/div/div[2]/div/div/div[3]/div/div[2]/div/div[3]/div/div/div/div[1]/div[4]/div/span/a/span")))

          select = Select(driver.find_element_by_xpath("/html/body/div[2]/div[2]/div/div[5]/div/div[2]/div/div/div[2]/div[1]/div/select"))
          select.select_by_visible_text("Date Added")
        
          # Descending 
          driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div[5]/div/div[2]/div/div/div[2]/div[1]/div/span/div/div/button[2]').click()
          content = driver.page_source
          soup = BeautifulSoup(content)
          t = soup.find('div', attrs={'class': 'gwt-Label CA6N44B-a-db CA6N44B-a-eb'})
          title_name = t.text
          # title_name = title_name.replace('/', '-')
          #print(title_name)
          time.sleep(10)
          if checkbox_inthe_page.is_displayed():


             count_for_pdfList += 1
             #print("check for pdf list, count is, ", count_for_pdfList)
             #print("Checkboxes are enabled, page contains pdf list")
             content = driver.page_source
             soup = BeautifulSoup(content)
             for l in soup.find_all('span', attrs={'class': 'gwt-InlineLabel pa-clicky'}):
                 print("Printing l")			
                 print(l)
                 project_name.append(title_name)
                 name = l
                 #get_title.append(name.text)
                 # print("**********************************************************************************************")
                 document_label = l.parent.parent.parent.parent.find_next_sibling()
                 #document_label=document_label.replace('"','\"')
			
                 #s=retString(document_label)
                 #print(s)				
                 print("Document Label")
                 get_document_label.append(document_label.text)
				
                 document_date = document_label.find_next_sibling()
                 get_document_date.append(document_date.text)
				
                 file_type = document_date.find_next_sibling()
                 get_file_type.append(file_type.text)
				
                 size = file_type.find_next_sibling()
                 get_size.append(size.text)
				
				
                 added_by = size.find_next_sibling()
                 get_added_by.append(added_by.text)				
				
                 print("Added By " + str(added_by))
                 date_added = added_by.find_next_sibling()

                
                 if ((date_added.text in get_date_added) and (name.text not in get_title)):
                    get_date_added.append(date_added.text)
                 elif ((date_added.text not in get_date_added) and (name.text in get_title)):
                    get_date_added.append(date_added.text)
                 elif ((date_added.text in get_date_added) and (name.text in get_title)):
                        result.append(date_added.text)
                 get_title.append(name.text)
                 print("Printing lists")
             #Increase Number of Scrolls
             for m in range(0,40):
                 for l in soup.find_all('a', href=True):
                     if l.has_attr('href'):
                        if l['href'] not in links:
                           print(l['href'])
                           links.append(l['href'])
                #break
                 for l in soup.find_all('span', attrs={'class': 'gwt-InlineLabel pa-clicky'}):
                    
                     project_name.append(title_name)
                     name = l
                     #print("HELLOOOOOOOOOOOOOOOOOOOOOOOO",name)
                     #get_title.append(name.text)
                     # print("**********************************************************************************************")
                     document_label = l.parent.parent.parent.parent.find_next_sibling()
                     #print(document_label)
                     get_document_label.append(document_label.text)

                     document_date = document_label.find_next_sibling()
                     get_document_date.append(document_date.text)
                     #print(document_date)
                     file_type = document_date.find_next_sibling()
                     get_file_type.append(file_type.text)
                     #print(file_type)
                     size = file_type.find_next_sibling()
                     get_size.append(size.text)
                     #print(size)
                     added_by = size.find_next_sibling()
                     get_added_by.append(added_by.text)
                     #print(added_by)
                     date_added = added_by.find_next_sibling()
                     #print(date_added)                    
                     if ((date_added.text in get_date_added) and (name.text not in get_title)):
                        get_date_added.append(date_added.text)
                     elif ((date_added.text not in get_date_added) and (name.text in get_title)):
                        get_date_added.append(date_added.text)
                     elif ((date_added.text in get_date_added) and (name.text in get_title)):
                        result.append(date_added.text)
                     get_title.append(name.text)
                 i = 16
                 element = "/html/body/div[2]/div[2]/div/div[5]/div/div[2]/div/div/div[3]/div/div[2]/div/div[3]/div/div/div/div[" + str(
                     i) + "]/div[4]/div/span/a/span"
                 #print(element)
                 driver.execute_script(
                    "window.document.evaluate('" + element + "', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.scrollIntoView(true)")
                 #time.sleep(10)
                 content = driver.page_source
                 soup = BeautifulSoup(content)
                 for l in soup.find_all('a', href=True):
                     #print("for i = 16",l)
                     if l.has_attr('href'):
                        if l['href'] not in links:
                           #print(l['href'])                        
                           links.append(l['href'])
                 for l in soup.find_all('span', attrs={'class': 'gwt-InlineLabel pa-clicky'}):
                    
                     project_name.append(title_name)
                     name = l
                     #get_title.append(name.text)
                     # print("**********************************************************************************************")
                     document_label = l.parent.parent.parent.parent.find_next_sibling()
                     # print(document_label)
                     get_document_label.append(document_label.text)

                     document_date = document_label.find_next_sibling()
                     get_document_date.append(document_date.text)

                     file_type = document_date.find_next_sibling()
                     get_file_type.append(file_type.text)

                     size = file_type.find_next_sibling()
                     get_size.append(size.text)

                     added_by = size.find_next_sibling()
                     get_added_by.append(added_by.text)

                     date_added = added_by.find_next_sibling()
                      
                     if ((date_added.text in get_date_added) and (name.text not in get_title)):
                        get_date_added.append(date_added.text)
                     elif ((date_added.text not in get_date_added) and (name.text in get_title)):
                        get_date_added.append(date_added.text)
                     elif ((date_added.text in get_date_added) and (name.text in get_title)):
                        result.append(date_added.text)
                     get_title.append(name.text)
                 # for non_duplicate_links in links:
                 #     if non_duplicate_links not in new_set_links:
                 #         new_set_links.append(non_duplicate_links)
                 # print("links after 1 itr", new_set_links)

                 i = 8
                 element = "/html/body/div[2]/div[2]/div/div[5]/div/div[2]/div/div/div[3]/div/div[2]/div/div[3]/div/div/div/div[" + str(
                    i) + "]/div[4]/div/span/a/span"
                 print(element)
                 driver.execute_script(
                    "window.document.evaluate('" + element + "', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.scrollIntoView(true)")
                 #time.sleep(10)
                 content = driver.page_source
                 soup = BeautifulSoup(content)
                 for l in soup.find_all('a', href=True):
                     print("for i = 8",l)
                     if l.has_attr('href'):
                        if l['href'] not in links:
                           links.append(l['href'])
                 for l in soup.find_all('span', attrs={'class': 'gwt-InlineLabel pa-clicky'}):
                    
                     project_name.append(title_name)
                     name = l
                     #get_title.append(name.text)
                     # print("**********************************************************************************************")
                     document_label = l.parent.parent.parent.parent.find_next_sibling()
                     # print(document_label)
                     get_document_label.append(document_label.text)

                     document_date = document_label.find_next_sibling()
                     get_document_date.append(document_date.text)

                     file_type = document_date.find_next_sibling()
                     get_file_type.append(file_type.text)

                     size = file_type.find_next_sibling()
                     get_size.append(size.text)

                     added_by = size.find_next_sibling()
                     get_added_by.append(added_by.text)

                     date_added = added_by.find_next_sibling()
                      
                     if ((date_added.text in get_date_added) and (name.text not in get_title)):
                        get_date_added.append(date_added.text)
                     elif ((date_added.text not in get_date_added) and (name.text in get_title)):
                        get_date_added.append(date_added.text)
                     elif ((date_added.text in get_date_added) and (name.text in get_title)):
                        result.append(date_added.text)
                     get_title.append(name.text)
                # for non_duplicate_links in links:
                #     if non_duplicate_links not in new_set_links:
                #         new_set_links.append(non_duplicate_links)
                # print("links after 2 itr", new_set_links)

                 i = 16
                 element = "/html/body/div[2]/div[2]/div/div[5]/div/div[2]/div/div/div[3]/div/div[2]/div/div[3]/div/div/div/div[" + str(
                    i) + "]/div[4]/div/span/a/span"
                # html / body / div[2] / div[2] / div / div[5] / div / div / div / div[3] / div / div[3] / div / div[2] / div / div[5] / div / div[5] / div / div[3] / div / div / div / div[1] / div[3] / div / span
                 print(element)
                 driver.execute_script(
                    "window.document.evaluate('" + element + "', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.scrollIntoView(true)")
                 #time.sleep(10)
                 content = driver.page_source
                 soup = BeautifulSoup(content)

                 for l in soup.find_all('a', href=True):
                     print("for i = 16 2nd time",l)
                     if l.has_attr('href'):
                        if l['href'] not in links:
                           links.append(l['href'])
                 for l in soup.find_all('span', attrs={'class': 'gwt-InlineLabel pa-clicky'}):
                
                     project_name.append(title_name)
                     name = l
                    #get_title.append(name.text)
                    # print("**********************************************************************************************")
                     document_label = l.parent.parent.parent.parent.find_next_sibling()
                    # print(document_label)
                     get_document_label.append(document_label.text)

                     document_date = document_label.find_next_sibling()
                     get_document_date.append(document_date.text)

                     file_type = document_date.find_next_sibling()
                     get_file_type.append(file_type.text)

                     size = file_type.find_next_sibling()
                     get_size.append(size.text)

                     added_by = size.find_next_sibling()
                     get_added_by.append(added_by.text)

                     date_added = added_by.find_next_sibling()
                      
                     if ((date_added.text in get_date_added) and (name.text not in get_title)):
                        get_date_added.append(date_added.text)
                     elif ((date_added.text not in get_date_added) and (name.text in get_title)):
                        get_date_added.append(date_added.text)
                     elif ((date_added.text in get_date_added) and (name.text in get_title)):
                        result.append(date_added.text)
                     get_title.append(name.text)                
                 # for non_duplicate_links in links:
                 #     if non_duplicate_links not in new_set_links:
                 #         new_set_links.append(non_duplicate_links)
                 # print("links after 3 itr", new_set_links)

                 i = 8
                 element = "/html/body/div[2]/div[2]/div/div[5]/div/div[2]/div/div/div[3]/div/div[2]/div/div[3]/div/div/div/div[" + str(
                     i) + "]/div[4]/div/span/a/span"
                 print(element)
                 driver.execute_script(
                    "window.document.evaluate('" + element + "', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.scrollIntoView(true)")
                 #time.sleep(10)
                 content = driver.page_source
                 soup = BeautifulSoup(content)

                 for l in soup.find_all('a', href=True):
                    print("for i = 8 2nd time",l)
                    if l.has_attr('href'):
                        if l['href'] not in links:
                           links.append(l['href'])
                 for l in soup.find_all('span', attrs={'class': 'gwt-InlineLabel pa-clicky'}):

                    project_name.append(title_name)
                    name = l
                    
                    # print("**********************************************************************************************")
                    document_label = l.parent.parent.parent.parent.find_next_sibling()
                    # print(document_label)
                    get_document_label.append(document_label.text)

                    document_date = document_label.find_next_sibling()
                    get_document_date.append(document_date.text)

                    file_type = document_date.find_next_sibling()
                    get_file_type.append(file_type.text)

                    size = file_type.find_next_sibling()
                    get_size.append(size.text)

                    added_by = size.find_next_sibling()
                    get_added_by.append(added_by.text)

                    date_added = added_by.find_next_sibling()
                      
                    if ((date_added.text in get_date_added) and (name.text not in get_title)):
                        get_date_added.append(date_added.text)
                    elif ((date_added.text not in get_date_added) and (name.text in get_title)):
                        get_date_added.append(date_added.text)
                    elif ((date_added.text in get_date_added) and (name.text in get_title)):
                        result.append(date_added.text)
                    get_title.append(name.text)
                 #print(len(links))
                 #print(list(links))
                 if '' in links:
                   links.remove('')
                 #print(len(links))
                 if len(links)==temp6:
                   break
                 if len(links)>temp6:
                    del links[temp6-2:]
                    print(len(links))
                    #print(links)
                    break
                 
            # for non_duplicate_links in links:
            #     if non_duplicate_links not in new_set_links:
            #         new_set_links.append(non_duplicate_links)
            # print("links after 4 itr", new_set_links)
          time.sleep(5)
        
          home = "//div[contains(text(),'HOME')]"
          element = WebDriverWait(driver,180).until(EC.presence_of_element_located((By.XPATH,home)))
          element.click()
          time.sleep(2)
          driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div[5]/div/div[1]/div/div[3]/div/div[3]/div/div[2]/div/div[3]/span/div/i').click()
          print("{} completed".format(show))
          #for temp in get_date_added:
          #    if temp not in date_time:
          #        date_time.append(temp)
          ##print("number of links for {} is {}".format(show,len([link for link in set(links) if str(link).startswith('https') and (str(link).endswith('pdf') or str(link).endswith('doc'))])))
          #print("number of dates are:",len(get_date_added))
          #print("result",len(result))
          #print("number of  date  for {} is {}".format(show,len([date for date in list(set(get_date_added))])))
          #print("new date printed", list(set(get_date_added)))
          # for all_lnk in set(links):
          #     if str(all_lnk).startswith('https'):
          #         new_set_links.append(all_lnk)
          ds_commons.logDSAudit('scenecronized',show.replace("'","''"),show.replace("'","''"),'Python','scenechronize Scripts Download_FNOR','L',vLOG_DATE,'Project file read Completed',v_batch_no) 
          ds_commons.logDSAudit('scenecronized',show.replace("'","''"),show.replace("'","''"),'Python','scenechronize Scripts Download_FNOR','L',vLOG_DATE,'Project file Downloaded Started',v_batch_no) 		
          proj_failed=[]
          link_failed=[]
          #print("Links")
          #print(links)
          dflinks = pd.DataFrame(links , columns=['url'])
          #print("Set links")
          dflinks.drop_duplicates(inplace=True)
		
          nlinks = dflinks['url'].values.tolist()
          #print(nlinks)	
          #time.sleep(100)

          for new_links in (nlinks):
             #print(new_links)		
             if str(new_links).startswith('https') and (str(new_links).endswith('pdf') or str(new_links).endswith('doc')):
                try:
                    #print("Download link " + new_links)				     
                    driver.get(new_links)                   					


                except Exception:
				   
                    proj_failed.append(show)
                    link_failed.append(new_links)
                    ds_commons.logDSAudit('scenecronized',show.replace("'","''"),new_links.replace("'","''"),'Python','scenechronize Scripts Download_FNOR','E',vLOG_DATE,str(Exception).replace("'","''"),v_batch_no) 					
             else:
                continue

          links.clear()
          nlinks.clear()
          # Check if all files are downloaded or not 
          while True: 		  
             files = ([i for i in os.listdir(vDownloadPath) if (i.endswith("crdownload"))])	
             time.sleep(6)	  
             if len(files)>0:
                time.sleep(1)
                continue
             else:
                break			 
			 
          ds_commons.logDSAudit('scenecronized',show.replace("'","''"),show.replace("'","''"),'Python','scenechronize Scripts Download_FNOR','L',vLOG_DATE,'Project file Downloaded Completed',v_batch_no) 		

		  # Moving to Project Specific folder
          # Prefix existing files in Shared Drive with "OLD_" and delete them later
          files = [i for i in os.listdir(vDownloadPath) if (not i.startswith("FNOR_")) and  (not i.startswith("parsed_")) and path.isfile(path.join(vDownloadPath, i))]
          #print("number of  files present", files)
          file_names=[]
          for file in files:
             print(file)		
             filename, extension = os.path.splitext(file)
             file_names.append(filename)	
		
          for file in files:
             print(file + " moving to " + destination) 		
             shutil.move(vDownloadPath + file, destination)
			
          time.sleep(5)
		
          createNewRNMThread(show,s_id, destination) 		

       except Exception as exp1:
          if ids2.empty:
            ids2 = pd.DataFrame([[show,show_id,batch_no]],columns=["Project","Project_id","Batch_No"])
          else:
            df2 = pd.DataFrame([[show,show_id,batch_no]],columns=["Project","Project_id","Batch_No"])
            ids2=ids2.append(df2, ignore_index = True)
          shutil.rmtree(destination)              
          #df2 = pd.DataFrame([[show,show_id,batch_no]],columns=["Project","Project_id","Batch_No"])
          #ids=ids.append(df2, ignore_index = True)
          print('ids2:')
          print(ids2)
          print(str(exp1).replace("'","''")) 	
          ds_commons.logDSAudit('scenecronized',show.replace("'","''"),show.replace("'","''"),'Python','scenechronize','E',vLOG_DATE,'Project scrapping process has failed ' + str(exp1).replace("'","''"),v_batch_no) 			
          driver.get("https://www.scenechronize.com/sc/#home")
          time.sleep(6)
          driver.find_element_by_xpath(home).click()
          time.sleep(3)
          driver.find_element_by_xpath(clear_search).click()
          time.sleep(3)
          continue
      if ids2.empty or qwe==2:
         break
      ids=ids2
      ids2=pd.DataFrame()
      qwe+=1
   if not ids2.empty:
      print("Following Projects needs to be re-run and have errors:")
      print(ids2)
   else:
      print("All the projects have been successfully downloaded")
   con.close()    

