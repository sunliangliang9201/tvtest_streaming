#/usr/bin/env python2.7
#coding=utf-8
import requests
import random
import time
base_url = 'http://103.15.200.3/logger.php'


while 1:
    time.sleep(3)
    timestamp = time.time()
    timestruct = time.localtime(timestamp)
    tt = time.strftime('%Y-%m-%d %H:%M:%S', timestruct)
    params = {'enc': '0',
              'appkey': 'tvtest',
              'ltype': ''.join(random.sample('zyxwvutsrqponmlkjihgfedcba', 4)),
              'log': '{"uuid":"00001123a3-ffff-ffffe7763000","imei":"androidId01000000be03ddeb",'
                     '"uid":"androidId00000000be03ddeb","userid":"-","ctp":"android",'
                     '"androidid":"00000000be03ddeb","mac":"00:00:00:00:00:00",'
                     '"mtype":"BAOFENG_TV MST_6A358","mos":"4.4.4","ver":"3.1.1.778","gid":"dev",'
                     '"unet":"0","itime":'+ tt +',"value":{"sn":"60000AM3M00G18561699_95D9",'
                                                '"t":"fui_usercenter",'
                                                '"patch_id":"-","r":"r153416368761800000208",'
                                                '"itime":'+ tt +',"userid":"",'
                                                                '"app_id":"fui_usercenter","usertype":"GUEST","llal":"0","ip":"0.0.0.0"}}',
              }
    try:
        res = requests.get(base_url, params=params)
        print res
    except Exception as e:
        print e
