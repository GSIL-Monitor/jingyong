import random


x=[]
def create_passwd():
    for i in range(6):
        x.append(str(random.randrange(0,10)))
    sec_num=''.join(x)
    return sec_num

