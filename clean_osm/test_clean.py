from clean_osm.cl import *
from pathlib import Path
import xml.etree.ElementTree as ET
import os
import dask
import multiprocessing

"""
need to make big top down test thing

also it would be cool to make parameterized tests...
"""



def test_download_data():
    """ http://overpass-api.de/api/map?bbox=-113.533,53.5324,-113.1952,53.5504
    """
    min_lat, max_lat, min_lon, max_lon =  53.5164, 53.5718, -113.5742,-113.4485
    map_url = 'http://overpass-api.de/api/map?bbox={0},{1},{2},{3}'.format(min_lon, min_lat, max_lon, max_lat)
    download_data(map_url)
    size = os.path.getsize('map.osm')
    print("map.osm size in MB", size / (1024 ** 2))
    assert(size > (50 * 1024**2))


def test_get_element():
    for i in range(3):
        e = next(get_element('map.osm'))
        assert( isinstance(e, ET.Element))
        assert( e.tag in ('node','way','relation'))

def test_element2dict():
    for i in range(3):
        e = next(get_element('map.osm'))
        assert( isinstance(e, ET.Element))
        assert( e.tag in ('node','way','relation'))
        d = element2dict(e)
        assert( isinstance(d,dict))
        assert( d['type'] in ('node','way','relation'))


def test_pf():
    # we assume that we're using the default file for pf
    assert(os.path.exists('map.osm'))

    b1 = pf(force_refresh=True)
    assert( os.path.exists('partition_0.osm') )
    b0 = db.read_text('partition_*.osm').map(ET.XML).map(element2dict)

    s0 = b0.take(10)
    s1 = b1.take(10)
    # s0 = b0.random_sample(0.2, random_state = 42).compute()
    # s1 = b1.random_sample(0.2, random_state = 42).compute()
    assert( s0 == s1 )

    assert( os.path.exists('map_slxml.osm'))
    s = b0.take(1)[0]
    print(s)
    for b in [b0,b1]:
        assert(isinstance(b.take(1)[0], dict))
        assert(b.npartitions == multiprocessing.cpu_count() )

    p = Path(".")
    partition_files = list(p.glob('partition_?.osm'))
    assert(len(partition_files) == multiprocessing.cpu_count())

    # count the number of top level elements are the same in the bag
    #  as in the file
    dd = defaultdict(int)
    for e in get_element('map.osm'):
        dd[e.tag] += 1

    tvf_d = {k:w for k,w in top_value_freqs('type', b1).compute()}

    for k in dd:
        assert(dd[k] == tvf_d[k] )


def test_top_value_freqs():
    b = pf()
    freqs = top_value_freqs('type',b)
    assert(freqs.count().compute() == 3)
    for f in freqs:
        assert(f[0] in ['node','relation','way'])
