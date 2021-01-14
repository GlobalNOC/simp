#!/usr/bin/python

# Non-robust script for synchronizing repo's test composites with composites installed on a host.
# Missing composites can be added and have template data_sets files generated for them.
# Does not work unless the current host has files in /etc/simp/comp/composites.d/.
import sys, json
from os import listdir, utime
from os.path import isfile, join, exists
from shutil import copyfile

host_path = '/etc/simp/comp/composites.d/'
test_path = './conf/composites/'
data_path = './conf/data_sets/'

def show_state(percent):
    percent = int(percent/2)
    text = '[{0}{1}] {2}%'.format('='*percent, ' '*(50-percent), percent*2)
    sys.stdout.write("\r{0}".format(text))
    sys.stdout.flush()

def copy_composite(f):
    copyfile(join(host_path, f), join(test_path, f))

def create_data_sets(f):
    f = f.replace('.xml', '.json')

    in_json  = {161: {'acme.grnoc.iu.edu':{'INSERT OID PREFIX.*': {'INSERT OID': {'value': 'INSERT VALUE', 'time': 689644800}}}}}
    out_json = {'acme.grnoc.iu.edu': [{'INSERT KEY':'INSERT VALUE', 'time': 689644800}]}

    for datatype in ['input','output']:
        path = join(data_path, datatype, f)
        if not exists(path):
            with open(path, 'a') as datafile:
                if datatype == 'input':
                    datafile.write(json.dumps(in_json, indent=4))
                else:
                    datafile.write(json.dumps(out_json, indent=4))


current    = [f for f in listdir(test_path) if isfile(join(test_path, f))]
composites = [f for f in listdir(host_path) if isfile(join(host_path, f))]

total_u = 0
total_m = 0
updates = []
missing = []
for c in sorted(composites):
    if c not in current:
        total_m += 1
        missing.append(c)
    else:
        total_u += 1
        updates.append(c)

print('UPDATES: {0}'.format(total_u))
print('MISSING: {0}'.format(total_m))

print('\nUpdating composites in t/conf/composites/ with host\'s composites from puppet...')
for i in range(len(updates)):
    c = updates[i]
    copy_composite(c)
    show_state(((i+1) / len(updates)) * 100)
print


if missing:

    print('\nThese composites are missing from testing and require new test files:')
    print('\n'.join(missing))

    add_files = raw_input('\nWould you like to create new test files for the missing composites? (y/n): ').strip().lower()

    if add_files == 'y':
        
        print('\nCopying composites and creating data_set input and output JSON files for missing composites...')
        for i in range(len(missing)):
            c = missing[i]
            copy_composite(c)
            create_data_sets(c)
            show_state(((i+1) / len(missing)) * 100)
        print

        print('Note: Remember to update the contents of new files in conf/data_sets!')

print('\nAll composite test files now synchronized with the host!\nTip: Run "git status" to see new or changed files.\n')
