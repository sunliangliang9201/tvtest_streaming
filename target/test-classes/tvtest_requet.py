#/usr/bin/env python2.7
#coding=utf-8
import requests
import random
import time
base_url = 'http://103.15.200.3/logger.php'

print 1
while 1:
    time.sleep(5)
    timestamp = time.time()
    timestruct = time.localtime(timestamp)
    tt = time.strftime('%Y-%m-%d %H:%M:%S', timestruct)
    print type(tt)
    ltpye = ''.join(random.sample('zyxwvutsrqponmlkjihgfedcba', 4))
    params = {'enc': '0',
              'appkey': 'tvtest',
              'ltype': "hello000",
              'log': '{"androidid":"000000002b3db3fb","unet":"3","mos":"5.1.1","mac":"2c:d9:74:04:08:d2","uuid":"00000000-1442-390e-ffff-ffffd545a83b","userid":"-","mtype":"BAOFENG_TV AML_T962","lau_ver":"4.0.4.1612","gid":"dev","imei":"000000002113131","itime":"2018-10-12 13:23:38","version":"3.0","uid":"000000002113131","value":{"pid":"heart","sn":"60000AM1F00D17CK0021_3F51","plt_ver":"V4.0.43","ip":"123.126.114.194","page_title":"com.bftv.fui.launcher.views.activity.IndexRootActivity","softid":"11173101","plt":"AML_T962","package_name":"com.kuyun.common.androidtv"},"apptoken":"282340ce12c5e10fa84171660a2054f8","ver":"3.0.9.368"}',
              }
    try:
        res = requests.get(base_url, params=params)
        print ltpye
    except Exception as e:
        print e

