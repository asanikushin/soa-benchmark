"""
Dependencies:
 
    pip install tabulate simplejson
 
"""

from timeit import timeit
from tabulate import tabulate
import sys

message = '''d = {
    'PackageID' : 1539,
    'PersonID' : 33,
    'Number': 0.1,
    'Name' : """MEGA_GAMER_2222""",
    'Inventory': dict((str(i),i) for i in iter(range(100))),  
    'CurrentLocation': """
        Pentos is a large port city, more populous than Astapor on Slaver Bay, 
        and may be one of the most populous of the Free Cities. 
        It lies on the bay of Pentos off the narrow sea, with the Flatlands 
        plains and Velvet Hills to the east.
        The city has many square brick towers, controlled by the spice traders. 
        Most of the roofing is done in tiles. There is a large red temple in 
        Pentos, along with the manse of Illyrio Mopatis and the Sunrise Gate 
        allows the traveler to exit the city to the east, 
        in the direction of the Rhoyne.
        """
}'''
avro_scheme = '''
scheme = {
    'name': 'data',
    'type': 'record',
    'fields': [
        {'name': 'PackageID', 'type': 'long'},
        {'name': 'PersonID', 'type': 'int'},
        {'name': 'Number', 'type': 'double'},
        {'name': 'Name', 'type': 'string'},
        {'name': 'Inventory', 'type': {'type': 'map', 'values': 'int'}},
        {'name': 'CurrentLocation', 'type': 'string'},
    ]
}  '''

avro_helpers = """from io import BytesIO
import fastavro
def serialize(schema, data):
    bytes_writer = BytesIO()
    fastavro.schemaless_writer(bytes_writer, schema, data)
    return bytes_writer.getvalue()

def deserialize(schema, binary):
    bytes_writer = BytesIO()
    bytes_writer.write(binary)
    bytes_writer.seek(0)

    data = fastavro.schemaless_reader(bytes_writer, schema)
    return data
"""

yaml_helpers = """from io import StringIO
import yaml

def serialize(data):
    return yaml.dump(data)
    
def deserialize(data):
    return yaml.unsafe_load(data)
"""

setup_pickle = '%s ; src = pickle.dumps(d, 2)' % message
setup_json = '%s ; src = json.dumps(d)' % message
setup_xml = '%s ; src = dict2xml(d, wrap="all")' % message
setup_avro = f"{avro_helpers}\n{message}\n{avro_scheme}\nsrc = serialize(scheme, d)"
setup_yaml = f"{yaml_helpers}\n{message}\nsrc = serialize(d)"

tests = [
    # (title, setup, enc_test, dec_test)
    ('pickle (native serialization)', 'import pickle; %s' % setup_pickle, 'pickle.dumps(d, 2)', 'pickle.loads(src)'),
    ('json', 'import json; %s' % setup_json, 'json.dumps(d)', 'json.loads(src)'),
    ('xml', 'from dict2xml import dict2xml; from lxml import objectify; %s ' % setup_xml, 'dict2xml(d, wrap="all")',
     'objectify.fromstring(src)'),
    ('avro', setup_avro, 'serialize(scheme, d)', 'deserialize(scheme, src)'),
    ('yaml', setup_yaml, 'serialize(d)', 'deserialize(src)'),
]

loops = 5000
enc_table = []
dec_table = []

print("Running tests (%d loops each)" % loops)

for title, mod, enc, dec in tests:
    src = None
    print(title)

    print("  [Encode]", enc)
    result = timeit(enc, mod, number=loops)
    exec(mod)
    enc_table.append([title, result, result / loops, sys.getsizeof(src)])

    print("  [Decode]", dec)
    result = timeit(dec, mod, number=loops)
    dec_table.append([title, result, result / loops])

enc_table.sort(key=lambda x: x[1])
enc_table.insert(0, ['Package', 'Seconds', 'Per loop', 'Size'])

dec_table.sort(key=lambda x: x[1])
dec_table.insert(0, ['Package', 'Seconds', 'Per loop'])

print("\nEncoding Test (%d loops)" % loops)
print(tabulate(enc_table, headers="firstrow"))

print("\nDecoding Test (%d loops)" % loops)
print(tabulate(dec_table, headers="firstrow"))
