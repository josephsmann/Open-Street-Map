#!/usr/bin/env python
"""
To confirm, you should be auditing your dataset for problems and performing cleaning operations through Python, and then only insert the cleaned data into your dataset of choice. Your workflow might look like the following:
1) Complete the Case Study for your chosen database type.
2) Select an OpenStreetMap region.
3) Take a sample of the region. This will make it easier to iterate on your code as you adapt the case study code for your own region.
I often suggest two sample sizes: one that is 1-10MB just to make sure that your code is working (and also for submitting with the project materials)
and one that is intermediate in size so that you can get a good idea of the biggest problems in the dataset.

4) Adapt the auditing code for your region, based on your sample(s). Make sure that you have update functions so that you can clean the data when it comes to the next step.
You should create new scripts to account for additional problems that you find in your dataset, or other investigations that seem necessary.

5) Adapt the preparation code for converting the data from XML to CSV or JSON, depending on chosen database. Make sure you import your update functions to clean the data as part of this step.
6) Run the conversion code on the full dataset and import the converted data into SQL or MongoDB as appropriate.
7) Explore the data using queries.
"""

import os
import requests
import pandas as pd
import multiprocessing
import xml.etree.ElementTree as ET
from collections import defaultdict
from dask import bag as db
import jmespath as jp
import json


######### acquire map.osm
def download_data(url, fn = 'map.osm', path='.'):
    """
    download_data will acquire an osm format file from url and save it, if
    we have not done so previously and will create directories as needed.

    parameters
    ----------
        url: a url that points to an overpass-api link
        fn: (optional) filename that the saved file will be called
        path: (optional) a directory path if we want a separate data directory

    returns
    -------
        nothing

    side effects
    ------------
        creates file with path given
    """
    if not os.path.exists(path):
        os.mkdir(path)

    full_path = os.path.join(path, fn)

    if not os.path.exists(full_path):
        response = requests.get(url)
        with open(full_path, 'wb') as f:
            f.write(response.content)

def get_element(osm_fn, tags=('node', 'way', 'relation')):
    """
    parameters
    ----------
        osm_file: (string) name of osm file
        tags: tuple of permitted tags to extract from file

    returns (generator)
    -------------------
        get_element is a python generator of xml elemnents having one of the
        types specified in 'tags' coming from the osm file 'osm_file'

        Reference:
        http://effbot.org/zone/element-iterparse.htm
    """
    with open(osm_fn) as osm_file:
        context = iter(ET.iterparse(osm_file, events=('start', 'end')))
        _event, root = next(context)
        for event, elem in context:
            if event == 'end':
                if elem.tag in tags:
                    yield elem
                    root.clear()
            else:
                pass


def pf(fn = "map.osm", force_refresh = False):
    """
    parameters
    ----------
    fn: (str) is the name of file that should exist

    returns:
    -------
    a dask bag of python dictionaries

    side-effect:
    -------
    create a single file eg. map_slxml.osm in which every line is a single xml element
    """

    assert(os.path.exists(fn))


    fn_prefix = fn.split('.')[0]

    mod_time_map = os.path.getmtime(fn)
    mod_time_p1 = os.path.getmtime(fn_prefix + '_partition_0.osm') if os.path.exists(fn_prefix + '_partition_0.osm') else 0
    if mod_time_map < mod_time_p1 and not force_refresh:
        print("partition files still fresh")
        b = db.read_text(fn_prefix+'_partition_*.osm')
    else:
        print("making fresh partition files 9")

        out_fn = fn_prefix + '_slxml.osm'

        # create file with one element per line
        with open(out_fn, 'w') as outputf:
            for i,e in enumerate(get_element(fn)):
                s = ET.tostring(e, encoding='unicode') # should not be binary
                s1 = ' '.join( s.strip().split() )
                outputf.write(s1+'\n')
        line_count = i

        num_cores = multiprocessing.cpu_count()
        partition_length = line_count // num_cores + 1
        fsize_bytes = os.path.getsize(out_fn)

        # create multi-partitioned dask bag from file with 1 element per line
        # it seems like we are obliged to strip the line ending...
        b = db.read_text(out_fn, fsize_bytes // num_cores + 1).map(str.strip)

        # create partitioned files from dask bag
        b.to_textfiles(fn_prefix + '_partition_*.osm')

    return b.map(ET.XML).map(element2dict)


# def partition_file(fn =  "map.osm"):
#     """
#     obsolete: replaced it with pf - might want to test if
#     pf and partition_file /
#
#     parameters
#     ----------
#         fn: file name of the osm file to be partitioned into the number
#         of processor cores available
#
#     effects
#     -------
#         partition_file will create the same number of partition files as processor
#         cores on the host machine  while retaining
#         XML element integrity for the purposes of parallel execution.
#
#         each 'node','way' and 'ref' element is on one line
#     """
#
#
#     mod_time_map = os.path.getmtime(fn)
#     mod_time_p1 = os.path.getmtime('sample_0.osm') if os.path.exists('sample_0.osm') else 0
#     if mod_time_map < mod_time_p1:
#         print("sample files still fresh")
#         return
#     else:
#         print("making fresh sample files")
#     # iteration count
#     it = 0
#     # character count
#     cc = 0
#     num_cores = multiprocessing.cpu_count()
#     file_size = os.path.getsize(fn)
#     partition_size = file_size // num_cores
#     SAMPLE_FILE = "sample_{}.osm"
#
#     # count_d will count the number of each kind of tag
#     count_d = defaultdict(int)
#     output = open(SAMPLE_FILE.format(it), 'wb')
#
#     for i, element in enumerate(get_element(fn)):
#         # if our file is too big we start a new one
#         if cc > partition_size:
#             output.close()
#             print(SAMPLE_FILE.format(it))
#             it += 1 # increment our sample file count
#             output = open(SAMPLE_FILE.format(it), 'wb')
#             cc = 0  # reset character  count to zero
#
#         # count the number of each kind of tag in this partition
#         count_d[element.tag] += 1
#         bs = ET.tostring(element, encoding='utf-8')
#         s = bs.decode()
#         s = ' '.join(s.rsplit())+'\n' #.replace('\n','')
#         cc += len(s)
#         output.write(s.encode())
#
#     output.close()
#     print(SAMPLE_FILE.format(it))
#     print(count_d)

def element2dict(e):
    """
    parameters
    ----------
    e is an instance of xml.etree.ElementTree.Element

    returns
    -------
    a dictionary with a minimal set of top-level
    keys: "type", "attr" and possibly 'tag'
    """
    d = {}
    d['type'] = e.tag
    d['attr'] = {}
    for k,v in e.attrib.items():
        d['attr'][k]= v
    d['tag'] = {}
    for c in e.findall("tag"):
        k = c.attrib['k']
        v = c.attrib['v']
        # if there is a ':' in the key name we
        # split the name in two and create a branch with the
        # prefix name and a sub-branch with the suffix name.
        if ':' in k:
            split_a = k.split(':')
            sk = split_a[0]
            sv = "_".join(split_a[1:])
            if sk not in d['tag']:
                d['tag'][sk] = {}
        #   if the leaf also needs to be a branch
        #   we store the leaf, and make a branch and
        #   attach the leaf value at "branch.root"
            if not(isinstance(d['tag'][sk], dict)):
                temp = d['tag'][sk]
                d['tag'][sk] = {'root': temp}
            # if sv in d['tag'][sk]:
            #     v = [d['tag'][sk][sv], v]
            try:
                d['tag'][sk][sv] = v
            except:
                print(sk, sv, v)
                print(d['tag'])
                print(ET.tostring(e, encoding = 'utf-8'))
                print('-----')
        else:
            d['tag'][k]  = v
    return d


def fix_city(doc):
    path = 'tag.addr.city'
    cpath = jp.compile(path)
    value = cpath.search(doc)
    if value:
        doc['tag']['addr']['city'] = 'Edmonton'

    return doc

def fix_province(doc):
    path = 'tag.addr.province'
    cpath = jp.compile(path)
    value = cpath.search(doc)
    if value:
        doc['tag']['addr']['province'] = 'Alberta'

    return doc

def fix_postal_codes(doc):
    path = 'tag.addr.postcode'
    pc_re = re.compile(r'([A-Z]\d[A-Z])[-\s]*(\d[A-Z]\d){0,1}', re.IGNORECASE)
    cpath = jp.compile(path)
    value = cpath.search(doc)
    if value:
        matches = pc_re.search(value)
        if matches:
            s = "{}".format(matches.group(1))
            if matches.group(2):
                s += " " + matches.group(2)
            s = s.upper()
            doc['tag']['addr']['postcode'] = s

    return doc

def top_value_freqs(subtag, b1, n=5):
    """
    parameters:
    ----------
    subtag (string): returns the top n frequencies of values
    for the key "subtag"
    if the value of "tag.[subtag]" is a dictionary then it returns
    the top frequencies of the keys of  that dictionary.

    b1 (dask Bag): the bag that will be searched

    n (int): number of the most freqent tags and their frequencies to return

    returns:
    -------
    a dask bag of pairs (value, count)
    """

    expression =  jp.compile(subtag)

    # make bag of subelements, remove None elements
    b2 = b1.map(lambda d: expression.search(d)).filter(lambda d: d)

    # make bag of lists comprised of key lists or just single element lists
    b3 = b2.map(lambda d: list(d.keys()) if isinstance(d, dict) else (d if isinstance(d, list) else [d]))

    # concatenate all lists together
    b4 = b3.flatten()

    # remove None elemens
    b5 = b4.filter(lambda d: d)

    # return bag with top n frequencies
    return b5.frequencies().topk(n, lambda x: x[1])



if __name__ == '__main__':
    min_lat, max_lat, min_lon, max_lon =  53.5164, 53.5718, -113.5742,-113.4485
    map_url = 'http://overpass-api.de/api/map?bbox={0},{1},{2},{3}'.format(min_lon, min_lat, max_lon, max_lat)
    download_data(map_url)
    # partition_files()
    b = pf()
    b.map(fix_city).map(fix_province).map(fix_postal_codes).map(json.dumps).to_textfiles('clean-*.json')
    # can we set up an iterator for this...
