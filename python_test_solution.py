import  pandas as pd
import os
import re
from bidi.algorithm import get_display
import codecs
import json


def Question_1(directory):
    flag =True
    file_num=0
    for filename in os.listdir(directory):
        file_num=file_num+1
        cdr_file = os.path.join(directory, filename)
        with open(cdr_file,'r') as f:
            for x in f:
                x = x.rstrip()
                js=json.loads(x)
                filename_split =filename.split('_')
                js['SourceFileName']=filename_split[1]
                js['FileServerIp'] = filename_split[0]
                if flag:
                     df_raw = pd.DataFrame(columns=js.keys())
                df_raw=df_raw.append(js, ignore_index=True)
                flag= False
        if file_num%3 ==0 :
            flag = True
            df_raw.rename(columns={'MedGotMsgFromApi':'med_got_msg_from_api', 'eventTimestamp':'event_timestamp', 'hostName':'host_name', 'origGT':'orig_gt', 'destGT':'dest_gt', 'reason':'reason', 'cdrType':'cdr_type', 'recordType':'record_type', 'callingNumber':'calling_number', 'calledNumber':'called_number', 'msgSubmissionTime':'msg_submission_time', 'clientId':'client_id', 'gmt1':'gmt1', 'originatingProtocol':'originating_protocol', 'gmt2':'gmt2', 'destinationProtocol':'destination_protocol', 'terminationCause':'termination_cause', 'transactionId':'transactionid', 'msgLength':'msg_length', 'concatenated':'concatenated', 'concatenatedFrom':'concatenated_from', 'concatenatedFromTotal':'concatenated_from_total', 'priority':'priority', 'sequence':'sequence_1', 'sequenceNumber':'sequence_number','operatorCodeFrom':'operator', 'packType':'pack_Type', 'packName':'pack_Name', 'sending_IP':'sending_IP', 'MTtags':'MTtags', 'accountID':'accountID',}, inplace=True)
            df_raw.to_csv(r'G:\My Drive\python_projects\Testing_new_code\biyond\Files_Question_1\\'+filename_split[0]+'_'+str(int(file_num / 3))+'.csv',sep='|',index=False,columns=['med_got_msg_from_api','event_timestamp','host_name','orig_gt','dest_gt','reason','cdr_type','record_type','calling_number','called_number','msg_submission_time','client_id','gmt1','originating_protocol','gmt2','destination_protocol','termination_cause','transactionid','msg_length','concatenated','concatenated_from','concatenated_from_total','priority','sequence_1','sequence_number','operator','pack_Type','pack_Name','sending_IP','MTtags','accountID','SourceFileName','FileServerIp'])
    return 'Files {} been created'.format(filename_split[0]+'.csv')




def Question_2(directory):
        for filename in os.listdir(directory):
            dict_file = {}
            with codecs.open(os.path.join(directory, filename), 'r', encoding='utf-8') as f:
                lines = f.read().splitlines()
                for i, line in enumerate(lines):
                    line = str(line)
                    if re.match(r"ERROR Problem executing: campaign", line):
                        campaign_id = re.search(r"ERROR Problem executing: campaign (.+?) for user", line).group(1)
                        campaign_name = "Sales Campaign" + str(filename.split('.')[-1])
                        dict_file = {campaign_id: campaign_name}

                        with open(directory + '\\' + 'camp-list.json', 'a') as file:
                            file.write("{}\n".format(json.dumps(dict_file)))

                        str_fix = get_display(str(lines[i - 2]))
                        date = re.search(r"@ (.+?): EXCEPTION", str_fix).group(1)
                        time = re.search(r'\d{2}:\d{2}:\d{2}:\d{2}', str_fix).group(0)
                        print('{} was failed on {} in time {}'.format(campaign_name, date, time))
        return True


if __name__ == '__main__':
    # test_commit
    path=(os.path.dirname(os.path.realpath(__file__)))
    print (Question_1(directory = path+'\\'+'Files_Question_1'))
#The attached Files_Question_2 encoding format been change to UTF-8
    print (Question_2(directory = path+'\\'+'Files_Question_2'))