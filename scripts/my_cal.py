import aipy as a, numpy as n

def get_aa(freqs):
    # Define location of instrument. 
    lat, lon = '38:25:59.24', '-79:51:02.1'
    #model of primary beam. 
    beam = a.fit.Beam(freqs)
    #list of antennas with requisite nanosecond locations, primary beams, and
    #any other calibration parameters you wish to provide.
    ants = [a.fit.Antenna( -8.48, 455.28, 9.82, beam),
            a.fit.Antenna( 205.47, 319.53, -251.71, beam),
            a.fit.Antenna( 187.10, -352.95, -232.59, beam),
            a.fit.Antenna( -262.70, -219.07, 318.70, beam),
            a.fit.Antenna( -8.48, 455.28, 9.82, beam),
            a.fit.Antenna( 205.47, 319.53, -251.71, beam),
            a.fit.Antenna( 187.10, -352.95, -232.59, beam),
            a.fit.Antenna( -262.70, -219.07, 318.70, beam)
    ]
    
#    pos = N.array([0, 0, 0],
#                  [1, 1, 1],
#                  [2, 2, 2],
#                  [3, 3, 3],
#                  [4, 4, 4], 
#                  [5, 5, 5], 
#                  [6, 6, 6], 
#                  [7, 7, 7]
#   )
#create AntennaArray at the specified location with the listed antennas
    aa = a.fit.AntennaArray((lat, lon), ants)
    return aa

def get_caltalog(srcs=None, cutoff=None):
    #use built in AIPY source catalog. Can substitute our own sources or source
    #calibrations.
    return a.src.get_catalog(srcs=srcs, cutoff=cutoff)
    
def get_walsh():
    #these are the orthogonal walsh codes for each of the 8 antennas. The fastest switch is 1/16th of a complete code (the base).       Note that the design only allows 8 antennas, but switching frequency may vary.
    Walsh = n.array([[0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1],[0,0,0,0,1,1,1,1,1,1,1,1,0,0,0,0],[1,1,1,1,0,0,0,0,1,1,1,1,0,0,0,0],[1,1,0,0,0,0,1,1,1,1,0,0,0,0,1,1],[0,0,1,1,1,1,0,0,1,1,0,0,0,0,1,1],[0,0,1,1,0,0,1,1,1,1,0,0,1,1,0,0],[1,1,0,0,1,1,0,0,1,1,0,0,1,1,0,0],[1,0,0,1,1,0,0,1,1,0,0,1,1,0,0,1]])

    return Walsh

