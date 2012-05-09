import numpy as N
#import matplotlib: matplotlib.use('TkAgg')
#import matplotlib.pyplot as plt
import optparse, os, sys, struct

o = optparse.OptionParser()
o.add_option('-i', '--ip', dest = 'ip', help = 'Roach serial number on paper1')
o.add_option('-w', '--walsh', dest = 'walsh', type='string', help = 'type of walsh pattern. 0= all zeros. all = 8 orth. pattern.')
opts,args = o.parse_args(sys.argv[1:])

pid = int(args[0])

brams = ['adc0_3', 'adc4_7', 'iadc0_3', 'iadc4_7']

def write_walsh(pid,pattern = opts.walsh):
    #to write walsh functions to bram
   # bram = open('/proc/%d/hw/ioreg/walsh_codes' %pid ,'w')
    bram = open('/proc/%d/hw/ioreg/walsh_codes' %pid ,'w')
    if pattern =='0':	   
	    walsh=[ 
		     [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
		     [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
		     [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
		     [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
		     [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
		     [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
		     [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
		     [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]]
    	    
    	    
	  # these are the walsh functions to use (down)
    if pattern == 'all':
    	walsh = [
          [1,0,0,1,1,0,0,1,1,0,0,1,1,0,0,1],
		  [1,1,0,0,1,1,0,0,1,1,0,0,1,1,0,0],
		  [0,0,1,1,1,1,0,0,1,1,0,0,0,0,1,1],
          [1,1,0,0,1,1,0,0,1,1,0,0,1,1,0,0],
		  [1,1,0,0,0,0,1,1,1,1,0,0,0,0,1,1],
		  [1,1,1,1,0,0,0,0,1,1,1,1,0,0,0,0],
		  [0,0,0,0,1,1,1,1,1,1,1,1,0,0,0,0],
		  [0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1]] 

    entries = 2048
    seq_len = len(walsh[0])
    rep = entries/(seq_len)
    c = 0
    list = []
    for i in range(seq_len):
        string=''
        for k in range(len(walsh)):
            a = walsh[k][i]
            b = str(a)
            string += b
  	print string
  	list.append(string) 
        number = int(string,2)
        inp = struct.pack('I', number)
        for l in range(rep):
            bram.seek(c)
            bram.write(inp)
            c += 4
#            print [inp]
#bram.close()

    
def data_receiver(bram):
    bytes = 8192 # Bram specific. Have 2^11 addresses with 4 bytes in each address.
    data = open(bram, 'w')
    data.seek(0)
    A0 = range(2048)
    A1 = range(2048) 
    A2 = range(2048) 
    A3 = range(2048)  
    for i in range(4):
        for k in range(bytes):
            seek(k)
            if k%4 == 0:
                d =  data.read(1)
                num = struct.unpack('b', d)
                A0[k/4] = num
            elif k%4 == 1:
                d = data.read(1)
                num = struct.unpack('b', d)
                A1[k/4] = num
            elif k%4 == 2:
                d = data.read(2)
                num = struct.unpack('b', d)
                A2[k/4] = num
            elif k%4 == 3:
                d = data.read(2)
                num = struct.unpack('b', d)
                A3[k/4] = num


def plot_thread(a):
    x = range(2048)
    plt.xlabel('time')
    plt.ylabel('volts')
    plt.title('time vs. volts')
    plt.plt(x,a)
    
    



write_walsh(pid)
#time.sleep(5)
#datareceiver('adc0_3')
#for i in range(4):
#    plot_thread('A%d', i)










