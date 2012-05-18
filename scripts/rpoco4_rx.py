#! /usr/bin/env python       
import spead as S, numpy as N, aipy as A, ephem      
import sys, optparse, time, threading, os, rpoco4
#import matplotlib; matplotlib.use('TkAgg')
#import matplotlib.pyplot as plt
import logging; logger = logging.getLogger('rpoco4')
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
o.add_option('-s' , '--shift' , dest='fftshift', type='int' , default=0xffff, help='sets the fft shift.default is to shift every stage.')
o.add_option('-e' , '--eq' , dest='eqcoeff', type='int' , default=1000, help='sets the equalization coeffiecients.')
o.add_option('-l' , '--acclen' , dest='acclen', type='int' , default=0x80000, help='sets the accumulation lenght = number of spectra to accumulate. Default is 2**28/512 = 1.3 seconds.')
o.add_option('-n', dest='noiseval', type='int', default = 0x2121, help='Value to write into input selectors. 0=adc, 1= noise1, 2 = noise2, 3 = digital zero. Note that for noise1 and noise2 to be different (independent) all seed values must be different (default))')
o.add_option('-f', dest='seedval', type='int', default = 0x11223344, help='seed values to write for digital noise generators.')

o.add_option('--sync', dest='sync', action='store_false', default=True,
        help='to send sync pulse up or not. ')
o.add_option('--fft', dest='fft', action='store_false', default=True,
        help='to set fft shift or not.')
o.add_option('--acc', dest='acc', action='store_false', default=True,
        help='to set acc length or not.')
o.add_option('--seed', dest='seed', action='store_false', default=True,
        help='to set seed values for digital noise or not.')
o.add_option('--noise', dest='noise', action='store_false', default=True,
        help='to set input selectors or not.')
o.add_option('--gain', dest='gain', action='store_false', default=True,
        help='to set gain registers or not.')

opts,args = o.parse_args(sys.argv[1:])   

#Set up reciever and item group.The arr variable is so that spead knows to unpack numpy arrays(added when new item group is added, instead of fmt and shape, add narray=arr). This makes unpacking faster, whenever we need it.  
arr = N.zeros(NCHAN)
arr = N.array(arr, dtype=N.int32) 

rx = S.TransportUDPrx(opts.port, pkt_count=4096)
ig = S.ItemGroup()                                       
for id in rpoco4.FPGA_TX_RESOURCES:                          
    bram,name,fmt,shape = rpoco4.FPGA_TX_RESOURCES[id]
    if name != 'acc_num':
        ig.add_item(name=name, id=id , fmt=fmt, shape=shape)
    else: ig.add_item(name=name, id=id, fmt=fmt, shape=[])
ig.add_item(name='data_timestamp', id=rpoco4.TIMESTAMP_ID,fmt=S.mkfmt(('u',64)),shape=[-1])


#

#the values of the items that need to be written too in miriad. 

#sdf=input('sdf(d)=')  change in frequency between channels. 
#sfreq=input('sfreq(d)=') starting frequency, off 0th channel. 
#nchan=input('nchan(i)=') number of channels
#inttime=input('inttime(d)=') integration time
#bandpass=input('bandpass=') array of size nchan x nants
inttime= (opts.acclen*2**9)*5e-9
sfreq = 0.200
sdf = -0.40/1024.
fre = N.arange(8,dtype = N.integer)
freqs = fre * 0.1      
nchan = NCHAN

syncup_pulse = opts.sync
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
        uv = A.miriad.UV('poco.uv.tmp', status = 'new')
        for v in rpoco4.UV_VAR_TYPES:
            uv.add_var(v, rpoco4.UV_VAR_TYPES[v])
        uv['history'] = 'rpoco4'
        uv['obstype'] = 'mixed'
        uv['source'] = 'zenith'
        uv['operator'] = 'rpoco4'
        uv['telescop'] = 'rpoco4'
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
        global syncup_pulse
        logger.info('RPOCO8-RX.rx_thread: Starting receiver thread')
        for heap in S.iterheaps(rx): 
            ig.update(heap)
            ig['acc_num']= ig.heap_cnt
            #print ig.keys()
            if syncup_pulse:
                if ig.heap_cnt > 0: continue
                elif ig.heap_cnt == 0:
                    syncup_pulse = False
            if ig.heap_cnt == 0:
                continue
            sec = ig['data_timestamp']/1000.0
            jd = self.unix2julian(sec)
            logger.info('RPOCO8-RX.rx_thread: Got HEAP_CNT=%d' % (ig.heap_cnt))
            #continue
            #data = N.zeros(shape = 1024, dtype = N.complex64)
            for name in self.now:
                data = N.zeros(shape = 1024, dtype = N.complex64)
                if name[0] == name[1]:
                    data.real = N.dstack((ig[name+'_er'],ig[name+'_or'])).flatten()
                    self.uv_update(name,data[::-1],jd)
                else:
                    data.real = N.dstack((ig[name+'_er'],ig[name+'_or'])).flatten()
                    data.imag = N.dstack((ig[name+'_ei'],ig[name+'_oi'])).flatten()
                    self.uv_update(name,data[::-1],jd)

            if c == 0:
                self.filename = 'poco.%f.uv'%jd
                print 'Writing to filename %s' %self.filename
            c += 1
            if c == 30:
                c = 0
                print 'closing uv file'
                self.close_uv(self.filename)
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
        itme=self.inttime/86400.0
        #There are some ~.1ms corrections to this that I am ignoring. These are due to the time it takes for the data to leave the roach to rx computer (transmission time).
        jd = (sec/86400.0) + 2440587.5 - .5*itme
        return jd
    def close_uv(self,filename):
        '''Close current UV file and rename to filename'''
        logger.info('RPOCO8-RX.rx_thread: Closing UV file and renaming to %s' %filename)
        del(self.uv)
        os.rename('poco.uv.tmp', filename)
  
#start up remote transmitter
tx=S.Transmitter(S.TransportUDPtx(opts.ip, opts.port))
#This always sets the eq's , acclen, and fftshift. Have a choice to send a sync pulse up or not. 
#INtrinsically you have a choice to set the eq, acclen, fftshift or not.. but no functionality shown here.
bsc = rpoco4.BorphSpeadClient(opts.myip, tx, fft_shift = opts.fftshift, eq_coeff = opts.eqcoeff , acc_length = opts.acclen, seed=opts.seed, noise=opts.noise,
                            sync=opts.sync, seed_values=opts.seedval, noise_value=opts.noiseval, acc=opts.acc, eq=opts.gain, fft=opts.fft)

dr = DataRecorder(sdf, sfreq, nchan, inttime, bandpass=None)
dr.open_uv()

try: dr.write_thread()
except(KeyboardInterrupt):
    logger.info('RPOCO8-RX: Got KeyboardInterrupt, shutting down')
finally:    
    #logger.info('RPOCO8-RX: Shutting down TX')
    #tx.end()
    logger.info('RPOCO8-RX: Shutting down RX')
    rx.stop()
    try: filenamee = dr.filename
    except(AttributeError): filenamee = 'poco.BADFILE.uv'
    logger.info('RPOCO8-RX: Closing UV file. Renaming to '+ filenamee)
    dr.close_uv(filenamee)
