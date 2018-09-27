import requests


SMS_USER = 'N9711284'
SMS_PASSWORD = 'woavp9cJq'
SMS_HOST = 'http://smssh1.253.com/msg/send/json'


data={'account': SMS_USER, 'password': SMS_PASSWORD}

def sendto(mobile, content):
    if isinstance(mobile, list):
        mobile = ','.join(mobile)
    data.update(phone=mobile, msg=content, uid=1)

    try:
        res = requests.post(SMS_HOST, json=data)
        if res.status_code != 200:
            return False

        res = res.json()
        return res.get('code', None) == '0'
    except Exception as e:
        print(11)
        # logger.exception('chuanglan send error: {%s: %s}' % (mobile, content))
        return False

