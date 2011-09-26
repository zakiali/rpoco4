#! /usr/bin/env python       
import spead as S, numpy as N, aipy as A, ephem      
import sys, optparse, time, threading, os, rpoco8
import matplotlib; matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import logging; logger = logging.getLogger('rpoco8')
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('spead').setLevel(logging.WARN)
"0-16 coeff, 17 coeff-en, 20-26 coeff-addr, 30-31 ant-pair-sel"         
import my_cal
   
NCHAN = 1024
NANT = 4
letters = 'abcd'
colors = ['black','red','blue','green','cyan','magenta','yellow','gray']

o = optparse.OptionParser()
o.add_option('-i','--ip', dest='ip', help='IP address of Pocket Correlator')       
o.add_option('-m','--myip', dest='myip', help='IP address of this computer')        
o.add_option('-p','--port', dest='port', type='int', help='UDP port to listen to')
opts,args = o.parse_args(sys.argv[1:])   

#Set up reciever and item group.The arr variable is so that spead knows to unpack numpy arrays(added when new item group is added, instead of fmt and shape, add narray=arr). This makes unpacking faster, whenever we need it.  
arr = N.zeros(NCHAN)
arr = N.array(arr, dtype=N.int32) 

rx = S.TransportUDPrx(opts.port, pkt_count=4096)
ig = S.ItemGroup()                                       
for id in rpoco8.FPGA_TX_RESOURCES:                          
    bram,name,fmt,shape = rpoco8.FPGA_TX_RESOURCES[id]
    if name != 'acc_num':
        ig.add_item(name=name, id=id , fmt=fmt, shape=shape)
    else: ig.add_item(name=name, id=id, fmt=fmt, shape=[])
ig.add_item(name='data_timestamp', id=rpoco8.TIMESTAMP_ID,fmt=S.mkfmt(('u',64)),shape=[-1])


#

#the values of the items that need to be written too in miriad. 

#sdf=input('sdf(d)=')  change in frequency between channels. 
#sfreq=input('sfreq(d)=') starting frequency, off 0th channel. 
#nchan=input('nchan(i)=') number of channels
#inttime=input('inttime(d)=') integration time
#bandpass=input('bandpass=') array of size nchan x nants
inttime=(2**30)*5e-9
sfreq = 0.200
sdf = -0.10/1024.
fre = N.arange(8,dtype = N.integer)
freqs = fre * 0.1      
nchan = NCHAN

c = 0

class DataRecorder(S.ItemGroup):
    def __init__(self,sdf,sfreq,nchan,inttime,bandpass=None):
        aa = my_cal.get_aa(freqs)   
        self.aa = aa
        now = [
               'aa', 'ab', 'ac', 'ad', 'bb', 'bc', 'bd', 'cc', 'cd',
               'dd'
              ]
        then = [
                'ee', 'ef', 'eg', 'eh', 'ff', 'fg', 'fh', 'gg', 'gh',
                'hh', 'ce', 'de', 'cf', 'df', 'ag', 'bg', 'ah', 'bh'
               ]
        self.now = now 
        self.then = then
        self.sdf = sdf
        self.sfreq = sfreq
        self.nchan = nchan
        self.inttime = inttime
        self.bandpass = bandpass
    def open_uv(self):
        '''Open a Miriad UV file for writing. Bandpass is the digital 
        equalization that needs to be divided from the output data, 
        with dimensions (nant, nchans) and dtype complex64'''
        uv = A.miriad.UV('TMP_FILE', status = 'new')
        for v in rpoco8.UV_VAR_TYPES:
            uv.add_var(v, rpoco8.UV_VAR_TYPES[v])
        uv['history'] = 'rpoco8'
        uv['obstype'] = 'mixed'
        uv['source'] = 'zenith'
        uv['operator'] = 'rpoco8'
        uv['telescop'] = 'rpoco8'
        uv['version'] = '0.1'
        ants = N.array([self.aa[i].pos for i in range(NANT)]).transpose()
        uv['antpos'] = ants.flatten()
        uv['nants'] = NANT
        uv['npol'] = 1
        uv['epoch'] = 2000.
        uv['nspect'] = 1
        uv['ischan'] = 1
        uv['veldop'] = uv['vsource'] = 0. 
        uv['longitu'] = self.aa.long
        uv['latitud'] = uv['dec'] = uv['obsdec'] = self.aa.lat
        uv['sfreq'] = uv['freq'] = uv['restfreq'] = self.sfreq
        uv['sdf'] = self.sdf
        uv['nchan'] = uv['nschan'] = self.nchan
        uv['inttime'] = self.inttime

        if self.bandpass is None:
            bandpass = N.ones(8*1024,dtype=N.complex)
        uv['bandpass']= bandpass.flatten()
        uv['nspect0'] = NANT
        uv['nchan0'] = self.nchan
        uv['ntau'] = uv['nsols'] = 0
        uv['nfeeds'] = 1
        uv['ngains'] = NANT*(uv['ntau'] + uv['nfeeds'])
        uv['freqs'] = (NANT,) + (self.nchan, self.sfreq, self.sdf) * NANT
        self.uv = uv
    def write_thread(self):
        '''Starts up reciever thread and parses through the data.
           Then writes the uv files '''
        global c
        logger.info('RPOCO8-RX.rx_thread: Starting receiver thread')
        for heap in S.iterheaps(rx): 
            ig.update(heap)
            ig['acc_num']= ig.heap_cnt
            #print ig.keys()
            sec = ig['data_timestamp']/1000.0
            jd = self.unix2julian(sec)
            logger.info('RPOCO8-RX.rx_thread: Got HEAP_CNT=%d' % (ig.heap_cnt))
            #continue
            #data = N.zeros(shape = 1024, dtype = N.complex64)
            for name in self.now:
                data = N.zeros(shape = 1024, dtype = N.complex64)
                if name[0] == name[1]:
                    data.real = N.dstack((ig[name+'_er'],ig[name+'_or'])).flatten()
                    self.uv_update(name,data,jd)
                else:
                    data.real = N.dstack((ig[name+'_er'],ig[name+'_or'])).flatten()
                    data.imag = N.dstack((ig[name+'_ei'],ig[name+'_oi'])).flatten()
                    self.uv_update(name,data,jd) 
            c += 1
            if c%300 == 0:
                filename = 'poco.' + str((time.time()/86400.0)+2440587.5) + '.uv'
                print 'closing uv file'
                self.close_uv(filename)
                print 'reopening new uv file'
                self.open_uv()
    def uv_update(self,name,data,jd):
        ''' updates the uv file for a given baseline'''
        i = letters.index(name[0]); j = letters.index(name[1])
        uvw = N.array([i,j,0], dtype = N.double)
        preamble = (uvw, jd, (i,j))
        data[-2] = 0 # to get rid of the dc offset. causes plots to be "quantized"
        data[-1] = 0
        data[0] = 0
        data[1] = 0
        self.uv['ra'] = self.uv['obsra'] = self.uv['lst'] = self.aa.sidereal_time()
        self.uv['pol'] = A.miriad.str2pol['xx']
        flags = N.zeros(data.shape, dtype = N.int)
        flags[-2] = 1.
        flags[-1] = 1.
        flags[0] = 1.
        flags[1] = 1.
        self.uv.write(preamble, data, flags = flags)

    def unix2julian(self,sec):
        ''' Converts unix time to julian date. sec is seconds from unix epoch.
             inttime shoud be in seconds.'''
        itme=inttime/86400.0
        #There are some ~.1ms corrections to this that I am ignoring. These are due to the time it takes for the data to leave the roach to rx computer (transmission time).
        jd = (sec/86400.0) + 2440587.5 - .5*itme
        return jd
    def close_uv(self, filename):
        '''Close current UV file and rename to filename'''
        logger.info('RPOCO8-RX.rx_thread: Closing UV file and renaming to %s' %filename)
        del(self.uv)
        os.rename('TMP_FILE', filename)
  
#start up remote transmitter
tx=S.Transmitter(S.TransportUDPtx(opts.ip, opts.port))
bsc = rpoco8.BorphSpeadClient(opts.myip, tx, fft_shift = 0x155, eq_coeff =16 ,acc_length = 0x20000000)# acc_length = 0x20000000)

dr = DataRecorder(sdf, sfreq, nchan, inttime, bandpass=None)
dr.open_uv()

try: dr.write_thread()
except(KeyboardInterrupt):
    logger.info('RPOCO8-RX: Got KeyboardInterrupt, shutting down')
finally:    
    logger.info('RPOCO8-RX: Shutting down TX')
    tx.end()
    logger.info('RPOCO8-RX: Shutting down RX')
    rx.stop()
    filename = 'poco.' + str((time.time()/86400.0)+2440587.5) + '.uv'
    logger.info('RPOCO8-RX: Closing UV file. Renaming to '+ filename)
    dr.close_uv(filename)
