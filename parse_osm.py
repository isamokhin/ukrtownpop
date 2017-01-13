import pandas as pd
import numpy as np
import re
from lxml import etree
from osmread import parse_file, Node
from datetime import datetime
from time import mktime

fpath = '/home/igor/Documents/ukraine-latest.osm'

# code from https://github.com/dezhin/osmread/blob/master/osmread/parser/xml.py
# code changed to catch only node tags!

def parse(fp):
    context = etree.iterparse(fp, events=('start', 'end'))

    # common
    _type = None
    _id = None
    _version = None
    _changeset = None
    _timestamp = None
    _uid = None
    _tags = None
    # node only
    _lon = None
    _lat = None
    # way only
    _nodes = None
    # relation only
    _members = None

    for event, elem in context:

        if event == 'start':
            attrs = elem.attrib
            if elem.tag == 'node':
                _id = int(attrs['id'])
                _version = int(attrs['version'])
                _changeset = int(attrs['changeset'])
                # TODO: improve timestamp parsing - dateutil too slow
                _tstxt = attrs['timestamp']
                _timestamp = int((
                    datetime(
                        year=int(_tstxt[0:4]),
                        month=int(_tstxt[5:7]),
                        day=int(_tstxt[8:10]),
                        hour=int(_tstxt[11:13]),
                        minute=int(_tstxt[14:16]),
                        second=int(_tstxt[17:19]),
                        tzinfo=None
                    ) - datetime(
                        year=1970,
                        month=1,
                        day=1,
                        tzinfo=None
                    )
                ).total_seconds())
                if 'uid' in attrs:
                    _uid = int(attrs['uid'])
                else:
                    _uid = '000'
                _tags = {}

                if elem.tag == 'node':
                    _type = Node
                    _lon = float(attrs['lon'])
                    _lat = float(attrs['lat'])
                elif elem.tag == 'way':
                    continue
                    _type = Way
                    _nodes = []
                elif elem.tag == 'relation':
                    continue
                    _type = Relation
                    _members = []

            elif elem.tag == 'tag':
                _tags[str(attrs['k'])] = str(attrs['v'])

            elif elem.tag == 'nd':
                if _nodes:
                    _nodes.append(int(attrs['ref']))
                else:
                    continue

            elif elem.tag == 'member':
                _members.append(
                    RelationMember(
                        str(attrs['role']),
                        {
                            'node': Node,
                            'way': Way,
                            'relation': Relation
                        }[attrs['type']],
                        int(attrs['ref'])
                    )
                )

        elif event == 'end':
            if elem.tag in ('node', 'way', 'relation'):
                args = [
                    _id, _version, _changeset,
                    _timestamp, _uid, _tags
                ]

                if elem.tag == 'node':
                    args.extend((_lon, _lat))

                elif elem.tag == 'way':
                    args.append(tuple(_nodes))

                elif elem.tag == 'relation':
                    args.append(tuple(_members))

                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

                yield _type(*args)
                
cols = ['lat', 'lon', 'place', 'postal_code', 'addr:postcode', 'name:prefix', 'name',
        'name:uk', 'name:en', 'name:ru', 'population', 'koatuu']
dfs = []
count = 0
for entity in parse(fpath):
    if 'koatuu' in entity.tags:
        town = {key: entity.tags[key] for key in entity.tags if key in cols}
        town['lat'] = entity.lat
        town['lon'] = entity.lon
        df = pd.DataFrame(town, index=[count])
        dfs.append(df)
    count += 1
    if count % 100000 == 0:
        print(count)
    if count > 45600000:
        data = pd.concat(dfs)
        data.to_csv('ukr_towns.csv')
        dfs = []
        
#data2 = pd.concat(dfs)
#data2.to_csv('ukr_towns2.csv')
