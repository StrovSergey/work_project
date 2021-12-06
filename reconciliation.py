import requests
import time
from typing import ItemsView, Sized, Text
from requests.api import request
import json
import pandas as pd
import sys
from datetime import datetime
from datetime import datetime, timedelta

print(sys.getrecursionlimit()) 
print('Введите токен авторизации.')

token = input()
token_len = len(str(token))
if token_len < 30 or token_len > 30:
  print('Введен не корректный токен авторизации')
  print('Введите любое значение для выхода')
  ext = input()
  sys.exit()
  date_user = input('Укажите дату выгрузки в формате yyyy-mm-dd \n')

try:
  date_user = datetime.strptime(date_user+'T00:00', "%Y-%m-%dT%H:%M")
  date_UTC = (date_user-timedelta(hours=3)).strftime('%Y-%m-%d')
  valid_date = time.strptime(date_UTC, '%Y-%m-%d')
except ValueError:
  print('Введена не корректная дата.')
  print('Введите любое значение для выхода')
  ext = input()
  sys.exit()

status = int(input('Введите значение 1 или 2. Выгрузить все статусы(1). выгрузить только revise require(2). \n'))
if status == 1:
  filter_by = 'filter_by=updated'
elif status==2:
  filter_by = 'filter_by=status%20in%20revise_require%26updated'
else:
  print('Введено не корректное значение')
  print('Введите любое значение для выхода')
  ext = input()
  sys.exit()

print('Укажите размер выгрузки. Например: 1, 5, 50, 100 но не более 200 транзакций')

limit = int(input())
if limit > 200: 
  print('Введенное значение выгрузки больше допустимого, укажите меньшее значение.')
  print('Введите любое значение для выхода')
  ext = input()
  sys.exit()

url = "https://api.psp.io/payment-gateway/v3/transactions?limit_to="+str(limit)+"&"+str(filter_by)+"%3E%3D"+str(date_UTC)+"T21%3A00%3A00Z%26"
headers = {
  'Authorization': 'Bearer '+token
}
response = requests.request("GET", url, headers=headers)
json_response_id_acc = json.loads(response.text)
d  = len(json_response_id_acc['transactions'])
print("Колличество транзакций "+str(d))

if d == 0:
  sys.exit('Транзакции не найдены')

array_id = [0] * d
array_created = [0] * d
array_channel_id = [0] * d
array_account_id = [0] * d
array_terminal_id = [0] * d
array_RC = [0] * d
array_RC_DESC = [0] * d
array_no_found = [0] * d
array_remote_id = [0] * d
array_remote_id_ext = [0] * d
array_remote_id2 = [0] * d
array_remote_id3 = [0] * d
array_remote_id4 = [0] * d
array_remote_id5 = [0] * d
array_cs8 = [0] * d
array_cs9 = [0] * d
i = 0
while i < d :  
  id = json_response_id_acc["transactions"][i]["id"]
  array_id[i] = id
  created = json_response_id_acc["transactions"][i]["created"]
  date_created_MSK = datetime.strptime(created, "%Y-%m-%dT%H:%M:%S.%fz")
  array_created[i] = date_created_MSK+timedelta(hours=3)
  channel_id = json_response_id_acc["transactions"][i]["channel_id"]
  array_channel_id[i] = channel_id
  account_id = json_response_id_acc["transactions"][i]["account_id"]
  array_account_id[i] = account_id
  remote_id = json_response_id_acc["transactions"][i]["remote_id"]
  array_remote_id[i] = remote_id
  remote_id_ext = json_response_id_acc["transactions"][i]["remote_id_ext"]
  array_remote_id_ext[i] = remote_id_ext
  remote_id2 = json_response_id_acc["transactions"][i]["remote_id2"]
  array_remote_id2[i] = array_remote_id2
  remote_id3 = json_response_id_acc["transactions"][i]["remote_id3"]
  array_remote_id3[i] = array_remote_id3
  remote_id4 = json_response_id_acc["transactions"][i]["remote_id4"]
  array_remote_id4[i] = remote_id4
  remote_id5 = json_response_id_acc["transactions"][i]["remote_id5"]
  array_remote_id5[i] = remote_id5
  cs8 = json_response_id_acc["transactions"][i]["cs8"]
  array_cs8[i] = cs8
  cs9 = json_response_id_acc["transactions"][i]["cs9"]
  array_cs9[i] = cs9

  url_psp = "https://api.psp.io/payment-gateway/v3/merchant_accounts?limit_to=1&filter_by=id%3D"+str(account_id)
  response_terminal = requests.request("GET", url_psp, headers=headers)
  json_response_terminal = json.loads(response_terminal.text)
  terminal_id = json_response_terminal['merchant_accounts'][0]['provider_terminal']['credentials']['terminal_id']
  array_terminal_id[i] = str(terminal_id)
  date_open = datetime.strptime(date_UTC, '%Y-%m-%d').strftime("%d.%m.%Y")
  url_open = "https://egw01.open.ru/proxy_frm/service"
  other_open = {"SERVS" : "STATUS" , "TERMINAL" : terminal_id, "DATE" : date_open, "ORDER" : id }
  response_open = requests.request("GET", url_open, params=other_open)
  json_response_open = json.loads(response_open.text)
  try :
    no_found = json_response_open['VALUES'][0]['error']
  except:
    no_found = 0
  if no_found =='No order/rrn information found' :
      array_no_found[i] = no_found
  else :
    rc_desc = json_response_open['VALUES'][0]['RC_DESC']
    array_RC_DESC[i] = rc_desc
    rc = json_response_open['VALUES'][0]['RC']
    array_RC[i] = rc
  time.sleep(1)
  i =  i + 1

tablet_exel = pd.DataFrame({  
  'ID транзакции':          array_id , 
  'Дата создания':          array_created,
  'channel_id':             array_channel_id,
  'account_id':             array_account_id ,
  'Номер терминала':        array_terminal_id ,
  'RC':                     array_RC ,
  'RC_DESC':                array_RC_DESC ,
  'Транзакция не найдена':  array_no_found ,
  'remote_id':              array_remote_id,
  'remote_id_ext':          array_remote_id_ext,
  'remote_id2':             array_remote_id2,
  'remote_id3':             array_remote_id3,
  'remote_id4':             array_remote_id4,
  'remote_id5':             array_remote_id5,
  'cs8':                    array_cs8,
  'cs9':                    array_cs9
})

print('*******************************************************************\n'+str(tablet_exel))

try:
  date_time = datetime.now().strftime("%d-%m-%Y %H-%M")
  tablet_exel.to_excel('.\\unloading_'+str(date_time)+'.xlsx',sheet_name=str(date_time),index = False)
  print('Файл '+'unloading_'+str(date_time)+' успешно записан.')
  ext = input()
  print('Введите любое значение для выхода')
except:
  print('Ошибка записи файла выгрузки. Закройте открытый файл выгрузки и повторите операцию.')
  print('Введите любое значение для выхода')
  ext = input()
