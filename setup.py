"""RPOCO4: Roach-based Pocket Correlator (4-input)"""
from distutils.core import setup
import os, glob

__version__ = '0.0.1'

def indir(dir, files): return [dir+f for f in files]
def globdir(dir, files):
    rv = []
    for f in files: rv += glob.glob(dir+f)
    return rv
if os.path.exists('/boffiles'): data_files = [('/boffiles',glob.glob('data/*.bof'))]
else: data_files = []

setup(name = 'rpoco4',
    version = __version__,
    description = __doc__,
    long_description = __doc__,
    license = 'GPL',
    author = 'Zaki Ali',
    author_email = 'zakiali88 at gmail.com',
    requires = ['numpy','spead','aipy'],
    package_dir = {'':'src'},
    py_modules = ['rpoco4'],
    scripts = glob.glob('scripts/*'),
    data_files = data_files,
)

