#! /usr/local/bin/env python
import spead as S
import optparse, sys, rpoco4, os
import logging; logger = logging.getLogger('rpoco4')
#import walsh_rx

o = optparse.OptionParser()
o.add_option('-p','--port', dest='port', type='int',help='SPEAD port for tx/rx')
o.add_option('-v', '--verbose', dest='verbose', action='store_true', help='Be verbose') 
o.add_option('-w', '--walsh', dest = 'walsh', type = 'string', default = 0, help = 'type of walsh pattern. 0=zeroes, all = 8 orth. patterns')
opts,args = o.parse_args(sys.argv[1:])
                                                        
if opts.verbose:
    logging.basicConfig(level=logging.DEBUG)
else:
    stream = logging.basicConfig(level=logging.INFO)    
logging.getLogger('spead').setLevel(logging.WARN)

p = os.popen('pgrep -f baopoco_2011_Oct_14_2122.bof')
s = p.readline()
if s != '':
    pid = int(s.strip())
else:    
    pid = rpoco4.start_bof(boffile ='/boffiles/baopoco_2011_Oct_14_2122.bof')
#pid = int(args[0])                                                 
logger.info('RPOCO4-RX: Started %s with pid=%d' % ('/boffiles/baopoco_2011_Oct_14_2122.bof', pid))


#walsh_rx.write_walsh(pid,pattern = opts.option)

try:                                        
  bss = rpoco4.BorphSpeadServer(pid)
  bss.add_item(name='data_timestamp', description='time stamp for data in ms', shape=[1])#dtype=S.mkfmt(('u',64)), shape=[1])       
  while True:
    logger.info('RPOCO4-RX: Listening to port %d' % (opts.port))    
    bss.listen(opts.port)                                          
    logger.info('RPOCO4-RX: Waiting for client...')               
    client = bss.get_client(opts.port)                      
    for heap in bss.iterheaps():   
        logger.debug('RPOCO8-4X: Sending a heap')    
        client.send_heap(heap)
        logger.debug('RPOCO8-4X: Heap sent')
    logger.info('RPOCO4-RX: Client disconnected')
    bss.stop()                                      
except(OSError): logger.fatal('RPOCO4-4X: Cannot start RPOCO4. FPGA already programmed')
except(KeyboardInterrupt):
    logger.info('RPOCO4-RX: Got KeyboardInterrupt.  Stopping')
    bss.stop()
#finally: rpoco8.end_bof(pid)
 
