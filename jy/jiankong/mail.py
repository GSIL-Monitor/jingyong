import yagmail


receiver=[]
sender='运行监控'
cc=[]



def notify(content):

    yag = yagmail.SMTP(user="security@dpandora.cn", password="Jingyong1234" , host="smtp.exmail.qq.com",
                       port="465")
    yag.send(receiver, sender, cc=cc, contents=content)