#!/import/pa-tools/anaconda/anaconda3/py38/bin/python

import re
import argparse
import datetime
import os
import sys
from collections import defaultdict

program  = os.path.basename(__file__)
FLOW_DIR = os.path.dirname( os.path.dirname(__file__))
cmdline = "%s %s" % (__file__, ' '.join(sys.argv[1:]))

#---------------------------------------------------
#  parsing command line argument
#---------------------------------------------------
parser = argparse.ArgumentParser(description='Parse run.0.log file...')
parser.add_argument("-i",    "--inputf", help="provide input file name (if different from run.0.log)")
args = parser.parse_args()

infile   = args.inputf if args.inputf else "run.0.log"

cwd = os.getcwd()
current_dirname = os.path.basename(cwd)
parent_dirname = os.path.basename(os.path.dirname(cwd))
if m := re.search(r'^benchmark_per_section_bs(\d+)_ss(.*)', current_dirname):
    bs = m.group(1)
    ss_list = m.group(2).split("_")
else:
    sys.exit(f"ERROR: please check your directory: {current_dirname} : no run.0.log!\n")

if m := re.search(r'^llama.*_(\d+b)_', parent_dirname):
    model = m.group(1)
else:
    sys.exit(f"ERROR: what's the model name?: {parent_dirname}\n")

hidden_dict = {
    '8b': 4096,
    '32b' : 5120,
    '70b' : 8192,
}

measured_cycles = {}
measured_sslist = []
model_sections = {}
fp = open(infile)
while line := fp.readline():
    if m := re.search(r'^section_id \[(\d+)\] and', line):
        section = m.group(1)
        # read nextline until find 'program_start_done'
        nextline = ""
        while not re.search(r'^latency:.*program_start_done: (\d+) cycles', nextline):
            nextline = fp.readline()
        m = re.search(r'.*program_start_done: (\d+) cycles', nextline)
        measured_cycles[section] = int(m.group(1))
    elif m := re.search(r'^Analyzing graph model_nocache_(\d+).*', line):
        ss = m.group(1)
        #model_name = "nocache_"+ss
        model_sections[ss] = []
        if ss not in ss_list:
            print("ERROR: unexpected seqence size!")
            continue
        else:
            measured_sslist.append(ss)
        # collect sections with long latency
        nextline = fp.readline()
        while re.search(r'^Section \d+ appears in multiple graphs', nextline):
            m = re.search(r'^Section (\d+)', nextline)
            model_sections[ss].append(m.group(1))
            nextline = fp.readline()
    else:
        pass
fp.close()

# output
print('''
BS      SS               Target  Sections Measured      Flag (Warning/Red)
==================================================================================''')
for ss in sorted(measured_sslist, key=int):
    #(BS/2) (due to DP2) * (hidden/8) * SS * 2(2 bytes/element) * 3(num_p2p communication) / (1.024^3) / 11.5 GB/s * 1.6 GHz
    theoretical_cycles = (int(bs)/2.0) * (hidden_dict[model]/8) * int(ss) * 2 * 3 / (1.024**3) / 11.5 * 1.6
    print("\nBS%-5s nocache_%-7s  %5.1fM  " % (bs, ss, theoretical_cycles/1e6), end='')
    indent = ""
    for section in model_sections[ss]:
        print("%s%-8s %.1fM" % (indent, section, measured_cycles[section]/1e6))
        indent = " " * 33


#print(f"MODEL: {model}b")
#print(f"BS: {bs}")
#print(f"SS: {ss}")
#exit(0)
