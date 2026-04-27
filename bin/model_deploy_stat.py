#!/usr/bin/env python

import yaml
import re
import argparse
import subprocess
import os
import sys
import glob
from collections import defaultdict

def parse_args():
    parser = argparse.ArgumentParser(description = """
This script extracts the model experts and PEF file information from YAML files.
It starts with the inference deployment specs from fast-snova-ai-*.tfvars files
and outputs the required information in table format.
""")
    parser.add_argument("tfvars_dir", nargs="?",    help="Provide directory that contains .tfvar files(can also be a single file for debug). Default=pwd if not provided", default=os.getcwd())
    parser.add_argument("-all_models_in_yaml", action="store_true",help="Print out all the models regardless whether it's deployed or not")
    parser.add_argument("-full",action="store_true",help="Generate output files: NODE_deploy_date.by_bundle.*")
    parser.add_argument("-prefix_outfile",action="store",help="output files name will be: <PREFIX>.*", metavar="<PREFIX>")
    parser.add_argument("-bundle", nargs="+", action="extend",help="Specify 1 or more yaml files, support wildcard *, or a single <>.flist, output in bundle_snapshot.txt", metavar="<YAML>", default=[])
    return parser.parse_args()

def extract_deployment_names(tfvar_files):
    dep_stat = defaultdict(dict)

    for tf in tfvar_files:
        dep_siteid = re.sub(r'.*/([^/]+)\.tfvars$', r'\1', tf)
        with open(tf, 'r') as f:
            content = f.read()

        # Extract YAML spec
        yaml_spec = re.search(r'sn_tenant_object\s*=\s*<<EOZ\n(.*?)\nEOZ', content, re.DOTALL)
        if not yaml_spec:
            print("eRROR: Could not find YAML specification in file")
            sys.exit(1)

        config = yaml.safe_load(yaml_spec.group(1))
        if not config or 'spec' not in config:
            print("eRROR: Invalid YAML structure")
            sys.exit(1)

        # Extract coe-values.yaml content
        if 'coe-values.yaml' not in content:
            print("ERROR: No coe-values.yaml found")
            sys.exit(1)

        # Parse the nested YAML
        coe_values = re.search(r'cat <<EOVAL > coe-values.yaml\n(.*?)\n\s*EOVAL', content, re.DOTALL)
        if not coe_values:
            print("eRROR: Could not extract coe-values.yaml")
            sys.exit(1)

        tmp = re.sub(r'^    ', '', coe_values.group(1), flags=re.MULTILINE)
        coe_config = yaml.safe_load(tmp)
        if not coe_config:
            print(f"ERROR: No 'coe-values.yaml' found in ...{dep_siteid}.tfvars, exit ...")
            sys.exit(1)
        elif ('inferenceDeploymentSpecs' not in coe_config) and (('bundles' not in coe_config) or ('bundleDeploymentSpecs' not in coe_config['bundles'])):
            print(f"WARN: No DeploymentSpecs found in '{dep_siteid}.tfvars', continue ...")
            continue

        # 'bundleDeploymentSpecs' is the official default mechanism
        if 'bundles' in coe_config and 'bundleDeploymentSpecs' in coe_config['bundles']:
            read_bundle_deployment_spec(coe_config['bundles']['bundleDeploymentSpecs'], dep_siteid, dep_stat)

        # 'inferenceDeploymentSpecs' is for temporary workaround/debug ?
        if 'inferenceDeploymentSpecs' in coe_config and coe_config['inferenceDeploymentSpecs']:
            read_bundle_deployment_spec(coe_config['inferenceDeploymentSpecs'], dep_siteid, dep_stat)

    # convert dep_stat to a new node_deploy_dict, which contains the sum per site, per YAML
    node_deploy_dict = {}
    node_deploy_dict['BUNDLE_DIR_PATH'] = defaultdict(dict)
    node_deploy_dict['SITE'] = defaultdict(dict)
    node_deploy_dict['YAML'] = defaultdict(dict)    #active_yaml_files
    node_deploy_dict['FULL_INFO'] = defaultdict(dict)
    node_deploy_dict['PREFILL'] = defaultdict(dict)
    node_deploy_dict['DECODE'] = defaultdict(dict)
    for YAML in dep_stat:
        total_simple = 0
        total_prefill = 0
        total_decode = 0
        for siteid in dep_stat[YAML]['SITE_ID']:
            prefill = 0
            decode = 0
            if 'total_min_replicas_dcb' in dep_stat[YAML]['SITE_ID'][siteid]:
                tcount = dep_stat[YAML]['SITE_ID'][siteid]['total_min_replicas_dcb']['total']
                prefill = dep_stat[YAML]['SITE_ID'][siteid]['total_min_replicas_dcb']['prefill']
                decode = dep_stat[YAML]['SITE_ID'][siteid]['total_min_replicas_dcb']['decode']
                total_prefill += prefill
                total_decode += decode
                total_simple += dep_stat[YAML]['SITE_ID'][siteid]['total_min_replicas_simple']['total']
            else:
                tcount = dep_stat[YAML]['SITE_ID'][siteid]['total_min_replicas_simple']['total']
                total_simple += tcount

            if tcount > 0:
                node_deploy_dict['SITE'][siteid][YAML] = tcount
                node_deploy_dict['YAML'][YAML][siteid] = tcount
            if prefill + decode > 0:
                node_deploy_dict['PREFILL'][YAML][siteid] = prefill
                node_deploy_dict['DECODE'][YAML][siteid] = decode

        if total_prefill + total_decode > 0:
            node_deploy_dict['FULL_INFO'][YAML] = f"{total_simple}|DCB:{total_prefill}:{total_decode}"
        else:
            node_deploy_dict['FULL_INFO'][YAML] = f"{total_simple}"

        # If bundle_dir not exist: read all yaml/tfvar files in same dir (hidden feature for debug purpose)
        if os.path.isdir(dep_stat[YAML]['BUNDLE_DIR_PATH']):
            node_deploy_dict['BUNDLE_DIR_PATH'][YAML] = dep_stat[YAML]['BUNDLE_DIR_PATH']
        else:
            node_deploy_dict['BUNDLE_DIR_PATH'][YAML] = args.tfvars_dir
            print(f"DEBUG: default bundle_dir structures not found in: ../../../../../../../fast-coe/helm/, look up YAML files in {args.tfvars_dir}/ ...")

    return node_deploy_dict

# read_bundle_deployment_spec
def read_bundle_deployment_spec(dep_specs, dep_siteid, dep_stat):
    for deploy in dep_specs:
        # bundle or inference deploy method
        yaml_name = deploy['name']
        if 'groups' in deploy:
            dep_stat[yaml_name]['BUNDLE_DIR_PATH'] = os.path.abspath(f"{args.tfvars_dir}/../../../../../../../fast-coe/helm/charts/bundles/bundles")
            groups = deploy['groups']
        else:
            dep_stat[yaml_name]['BUNDLE_DIR_PATH'] = os.path.abspath(f"{args.tfvars_dir}/../../../../../../../fast-coe/helm/inference-deployments")
            groups = deploy['replicaGroups']

        total_min_replicas_simple = 0
        total_min_replicas_prefill = 0
        total_min_replicas_decode = 0
        for group in groups:
            if 'minReplicas' in group:
                total_min_replicas_simple += group['minReplicas']
            if 'continuous_batching' in group and 'prefill' in group['continuous_batching'] and 'decode' in group['continuous_batching']:
                total_min_replicas_prefill += group['continuous_batching']['prefill']['minReplicas']
                total_min_replicas_decode += group['continuous_batching']['decode']['minReplicas']

        # save dep_stat dictionary
        if 'SITE_ID' not in dep_stat[yaml_name]:
            dep_stat[yaml_name]['SITE_ID'] = defaultdict()
        if dep_siteid not in dep_stat[yaml_name]['SITE_ID']:
            dep_stat[yaml_name]['SITE_ID'][dep_siteid] = {}
        dep_stat[yaml_name]['SITE_ID'][dep_siteid]['total_min_replicas_simple'] = {'total': total_min_replicas_simple}
        if total_min_replicas_prefill + total_min_replicas_decode > 0:
            dep_stat[yaml_name]['SITE_ID'][dep_siteid]['total_min_replicas_dcb'] = {'total': total_min_replicas_prefill + total_min_replicas_decode}
            dep_stat[yaml_name]['SITE_ID'][dep_siteid]['total_min_replicas_dcb'].update({'prefill': total_min_replicas_prefill})
            dep_stat[yaml_name]['SITE_ID'][dep_siteid]['total_min_replicas_dcb'].update({'decode': total_min_replicas_decode})

# extract_pef_mapping
def extract_pef_mapping(node_deploy_dict):
    nested_defaultdict = lambda: defaultdict(nested_defaultdict)
    results = nested_defaultdict()
    PEF_ids = defaultdict(dict)
    
    for YAML in node_deploy_dict['YAML']:
        min_replica = sum( node_deploy_dict['YAML'][YAML].values() )
        if min_replica == 0: continue

        # figure out which yaml_file to use:
        bundle_dir = node_deploy_dict['BUNDLE_DIR_PATH'][YAML]
        yaml_file1 = f"{bundle_dir}/sambastack/{YAML}.yaml"
        yaml_file2 = f"{bundle_dir}/prod/{YAML}.yaml"
        if os.path.isfile(yaml_file1):
            yaml_file = yaml_file1
            if os.path.isfile(yaml_file2):
                print(f"Warning: duplicate file: '{YAML}.yaml' is found at both dirs: {bundle_dir}/{{sambastack,prod}}/")
                exit_code = subprocess.call(f"diff {yaml_file1} {yaml_file2}", shell=True, stdout=subprocess.DEVNULL)
                if exit_code:
                    print("       : with different content")
                else:
                    print("       : with identical content")
        elif os.path.isfile(yaml_file2):
            yaml_file = yaml_file2
        else:
            print(f"Warning: File {YAML}.yaml not found at {bundle_dir}/{{sambastack,prod}}/ Skip ...")
            continue
    
        # read_yaml_file and build results dictionary
        try:
            with open(yaml_file, 'r') as f:
                docs = yaml.safe_load_all(f)
                for yaml_doc in docs:
                    if yaml_doc['kind'] == 'BundleTemplate':
                        read_bundle_depoly_yaml(yaml_file, yaml_doc, results, PEF_ids)
                    elif yaml_doc['kind'] == 'InferenceDeployment':
                        read_inference_depoly_yaml(yaml_file, yaml_doc, results, PEF_ids)
                    else:
                        pass
        except FileNotFoundError:
            print(f"Warning: {yaml_file} not found!")

    return results, PEF_ids

# resolve_inline_pef: resolve a pef_key from an inline pefs dict; returns (pef_id, ss, batch_sizes) or (None, None, None) on error
def resolve_inline_pef(pefs, pef_key, seq_size, config, yaml_file):
    if pef_key not in pefs:
        print(f"ERROR: {pef_key} is not found in file: {yaml_file}")
        return None, None, None

    pef_source = re.sub(r'^.*/pefs/', '', pefs[pef_key]['source'])
    pef_id = pef_source.split('/')[0]

    # find out ss and batch_sizes
    if seq_size == '':
        extracted, count = re.subn(r'.*(\d+K)_PEF.*', r'\1', pef_key, flags=re.IGNORECASE)
        seq_size = extracted if count else '4k'
    batch_size = config.get('batch_size')
    if 'dynamic_dims' in config:
        decode_ssmin = config['dynamic_dims']['decode_seq']['min']
        decode_ssmax = config['dynamic_dims']['decode_seq']['max']
        decode_sstep = config['dynamic_dims']['decode_seq']['step']
        ssmin = round(decode_ssmin / 1024)
        ssmax = round(decode_ssmax / 1024)
        sstep = round(decode_sstep / 1024)
        ss = f'{seq_size}-DYT:{ssmin}k:{ssmax}k:{sstep}k'
        batch_sizes = config['dynamic_dims']['batch_size']['values'] if 'batch_size' in config['dynamic_dims'] else [batch_size]
    else:
        ss = seq_size
        batch_sizes = [batch_size]

    return pef_id, ss, batch_sizes

def read_bundle_depoly_yaml(yaml_file, yaml_doc, results, PEF_ids):
    #valid_models = r'^(Meta-Llama|Llama|DeepSeek|Whisper|GPT|Qwen|E5-Mistral|allam)'
    valid_models = r'.*'
    ignore_names = r'-Ricoh-|-Guard-|-Draft|SCX-Magpie-120b|UD_Humanizer_V(12.4|13|13.1)'
    _pt, YAML = re.findall(r'([^/]+)/([^/]+).yaml', yaml_file)[0] if re.search(r'/', yaml_file) else ['.', re.sub(r'\.yaml$', '', yaml_file)]
    prodtype = f"bundles/{_pt}"

    # Extract models/experts section
    models = yaml_doc.get('spec', {}).get('models', {})
    pefs = yaml_doc.get('spec', {}).get('pefs', {})
    
    for model_id in models:
        if not args.all_models_in_yaml and not re.search(f'{valid_models}', model_id, re.IGNORECASE): continue
        if not args.all_models_in_yaml and re.search(f'{ignore_names}', model_id): continue

        experts = models[model_id]['experts']
        for seq_size in experts:
            served_bs = defaultdict(list)

            for config in experts[seq_size]['configs']:
                pef_key = config['pef']

                # Extract just the 'PEF_id' from the full path; record the batch_sizes being served by this pef_id
                pef_id, ss, batch_sizes = resolve_inline_pef(pefs, pef_key, seq_size, config, yaml_file)
                if pef_id is None:
                    continue
                PEF_ids[pef_id][model_id] = 1

                for bs in batch_sizes:
                    served_bs[pef_id].append(bs)
                    results[model_id][ss]['all_batch_sizes'][bs][YAML] = True

            results[model_id][ss][YAML] = served_bs
            results[model_id][ss]['all_yaml_files'][YAML] = prodtype

def read_inference_depoly_yaml(yaml_file, yaml_doc, results, PEF_ids):
    #valid_models = r'^(Meta-Llama|Llama|DeepSeek|Whisper|GPT|Qwen|E5-Mistral|allam)'
    valid_models = r'.*'
    ignore_names = r'-Ricoh-|-Guard-|-Draft|SCX-Magpie-120b|UD_Humanizer_V(12.4|13|13.1)'
    _pt, YAML = re.findall(r'([^/]+)/([^/]+).yaml', yaml_file)[0] if re.search(r'/', yaml_file) else ['.', re.sub(r'\.yaml$', '', yaml_file)]
    prodtype = f"inference-deployments/{_pt}"

    # Extract models/experts section
    models = yaml_doc.get('spec', {}).get('experts', {})
    pefs = yaml_doc.get('spec', {}).get('pefs', {})
    
    for model_string in models:
        if not args.all_models_in_yaml and not re.search(f'{valid_models}', model_string, re.IGNORECASE): continue
        if not args.all_models_in_yaml and re.search(f'{ignore_names}', model_string): continue

        model_id, seq_size = re.findall(r'^(.*?)-?(\d+[kK])?$', model_string)[0]
        served_bs = defaultdict(list)

        for config in models[model_string]:
            pef_key = config['pef']

            # Extract just the 'PEF_id' from the full path; record the batch_sizes being served by this pef_id
            pef_id, ss, batch_sizes = resolve_inline_pef(pefs, pef_key, seq_size, config, yaml_file)
            if pef_id is None:
                continue
            PEF_ids[pef_id][model_id] = 1

            for bs in batch_sizes:
                served_bs[pef_id].append(bs)
                results[model_id][ss]['all_batch_sizes'][bs][YAML] = True

        results[model_id][ss][YAML] = served_bs
        results[model_id][ss]['all_yaml_files'][YAML] = prodtype

# a custom key function for sorted()
def sort_ss(ss):
    if ss == 'default':
        return 0
    elif m := re.search(r'^(\d+)[kK]-DYT:', ss):
        return 9000 + int(m.group(1))
    else:
        return int(re.sub(r'^(\d+).*', r'\1', ss))

# Print results
# - CLOUD_prod_models.csv
# - CLOUD_snapshot.csv
def print_results_model_offering(results, txtf="CLOUD_snapshot.txt", csv2="CLOUD_snapshot.csv", csv1="CLOUD_prod_models.csv"):
    rawf1 = txtf if txtf == "/dev/null" else f"{args.prefix_outfile}{txtf}"
    rawf2 = csv2 if csv2 == "/dev/null" else f"{args.prefix_outfile}{csv2}"
    prodf = csv1 if csv1 == "/dev/null" else f"{args.prefix_outfile}{csv1}"
    model_rawf1 = open(f"{rawf1}", "w")
    model_rawf2 = open(f"{rawf2}", "w")
    model_prodf = open(f"{prodf}", "w")
    model_rawf2.write("MODEL_ID,SEQENCE_SIZE,BATCH_SIZE,BUNDLE_YAML,PEF_ID\n")
    model_prodf.write("MODEL_ID,SEQENCE_SIZE,BATCH_SIZE\n")

    for model_id in sorted(results, key=str.lower):
        model_rawf1.write(f"{model_id} {'#'*132}"[:132] + "\n")
        model_id_str = model_id
        for context_ss in sorted(results[model_id], key=sort_ss):
            # sort out bs_list, yaml_list and pef_list
            bs_list = sorted(list(results[model_id][context_ss]['all_batch_sizes']), key=int)
            yaml_list = sorted(list(results[model_id][context_ss]['all_yaml_files']))
            pef_list = []
            consolidate = True
            common_pef_id = None
            for YAML in yaml_list:
                served_bs = results[model_id][context_ss][YAML]
                if len(served_bs) > 1 or len(list(served_bs.values())[0]) != len(bs_list):
                    consolidate = False
                out_str = ''
                for pef_id, bs in served_bs.items():
                    out_str += f"BS-{'-'.join(str(n) for n in bs)}: {pef_id}    "
                    if common_pef_id is None:
                        common_pef_id = pef_id
                    elif pef_id != common_pef_id:
                        consolidate = False
                pef_list.append(f"{out_str[:-4]}")
            if consolidate:
                for i in range(len(pef_list)):
                    pef_list[i] = common_pef_id if i == 0 else ""

            # print txt format lines foreach context_len/SS
            max_num_of_lines = max(len(bs_list), len(yaml_list))
            for i in range(max_num_of_lines):
                # batch_size
                if i == 0:
                    SS_string = context_ss if context_ss == "default" else f"SS-{context_ss}"
                    model_rawf1.write(f"    {SS_string:<24}{bs_list[0]:>4}    ")
                elif i < len(bs_list):
                    model_rawf1.write(f"{bs_list[i]:>32}    ")
                else:
                    model_rawf1.write(f"{' '*36}")
                # pef information
                if i < len(yaml_list):
                    prodtype = results[model_id][context_ss]['all_yaml_files'][yaml_list[i]]
                    yaml_file = f"{prodtype}/{yaml_list[i]}.yaml"
                    model_rawf1.write(f"{yaml_file:<72} {pef_list[i]}\n")
                else:
                    model_rawf1.write("\n")

            # print csv lines
            model_id_csv = model_id_str
            if m := re.search(r'^(\S+)-DYT:(\d+)k:(\d+)k:(\d+)k', context_ss):
                context_ss_csv1 = f'{m.group(1)}-DYT:{m.group(2)}k(min):{m.group(3)}k(max):{m.group(4)}k(step)'
            else:
                context_ss_csv1 = context_ss
            context_ss_csv2 = context_ss
            bs_list_csv = str(bs_list[0])
            yaml_list_csv = f"{results[model_id][context_ss]['all_yaml_files'][yaml_list[0]]}/{yaml_list[0]}.yaml"
            pef_list_csv = pef_list[0]
            for i in range(1, max_num_of_lines):
                model_id_csv += "\n" if model_id_csv else ""
                context_ss_csv2 += "\n"
                bs_list_csv += f"\n{str(bs_list[i])}" if i < len(bs_list) else "\n"
                yaml_list_csv += f"\n{results[model_id][context_ss]['all_yaml_files'][yaml_list[i]]}/{yaml_list[i]}.yaml" if i < len(yaml_list) else "\n"
                pef_list_csv += f"\n{pef_list[i]}" if i < len(pef_list) else "\n"
            model_prodf.write(f'{model_id_str},{context_ss_csv1},' + ' '.join((str(n) for n in bs_list)) + '\n')
            model_rawf2.write(f'"{model_id_csv}","{context_ss_csv2}","{bs_list_csv}","{yaml_list_csv}","{pef_list_csv}"\n')
            model_id_str = ""

            # print a separator line to separate another context_len/SS
            model_rawf1.write(f'    {"-"*100}\n')

        # print an empty line to separate another model
        model_rawf1.write("\n")

    # final summary line
    model_rawf1.write(f"Total models found: {len(results)}\n")
    model_rawf1.close()
    model_rawf2.close()
    model_prodf.close()

# print_results_PEF_summary
# - PEF_summary.txt
def print_results_PEF_summary(PEF_ids):
    result_dict = {}
    for pef in PEF_ids:
        models = ','.join(sorted(PEF_ids[pef]))
        result_dict[pef] = models

    with open(f"{args.prefix_outfile}PEF_summary.txt", "w") as f:
        for pef, models in sorted(result_dict.items(), key=lambda item: (item[1].lower(), item[0])):
            f.write(f"{pef:<32} {models}\n")

# print_results_yaml_summary
# - YAML_summary.txt
# - NODE_count_summary.by_bundle.txt
# - NODE_count_summary.by_site.txt
# - NODE_count_summary.by_model.txt
def print_results_yaml_summary(results, node_deploy_dict):
    active_yaml_files = {}
    inactive_yaml_files = {}

    all_bundle_files = []
    for bundle_dir in set(list(node_deploy_dict['BUNDLE_DIR_PATH'].values())):
        all_bundle_files.extend(glob.glob(f"{bundle_dir}/sambastack/*.yaml"))
        all_bundle_files.extend(glob.glob(f"{bundle_dir}/prod/*.yaml"))
    for yaml_file in all_bundle_files:
        YAML = re.sub(r'\.yaml$', '', re.sub(r'.*/', '', yaml_file))
        if YAML in node_deploy_dict['YAML']:
            active_yaml_files[YAML] = sum( node_deploy_dict['YAML'][YAML].values() )
        else:
            inactive_yaml_files[YAML] = 0

    # print YAML_summary.txt
    with open(f"{args.prefix_outfile}YAML_summary.txt", "w") as f:
        f.write("### ACTIVE YAML files: ---------------------------------------------------\n")
        for YAML in sorted(active_yaml_files, key=str.lower):
            yaml_file = f"{YAML}.yaml"
            total_deploy = active_yaml_files[YAML]
            persite_deploy = re.sub(r'fast-snova-ai-', '', str(node_deploy_dict['YAML'][YAML]))
            f.write(f"{yaml_file:<52} : {total_deploy} {persite_deploy}\n")

        f.write("\n")
        f.write("### IN-ACTIVE YAML files: ---------------------------------------------------\n")
        for YAML in sorted(inactive_yaml_files, key=str.lower):
            f.write(f"{YAML}.yaml\n")

    # print NODE_count_summary.*.txt
    with open(f"{args.prefix_outfile}NODE_count_summary.by_bundle.txt", "w") as f:
        f.write(f"{'YAML_FILE':<52}{'TOTAL':<12}")
        for site_id in node_deploy_dict['SITE']:
            f.write(f"{re.sub(r'^fast-snova-ai-', '', site_id):<8}")
        f.write("\n" + "-"*90 + "\n")
        final_totals = defaultdict(int)
        for YAML in sorted(active_yaml_files, key=str.lower):
            yaml_file = f"{YAML}.yaml"
            total_count = sum(node_deploy_dict['YAML'][YAML].values())
            f.write(f"{yaml_file:<52}{total_count:<12}")
            final_totals['total'] += total_count
            for site_id in node_deploy_dict['SITE']:
                if site_id in node_deploy_dict['YAML'][YAML]:
                    f.write(f"{node_deploy_dict['YAML'][YAML][site_id]:<8}")
                    final_totals[site_id] += node_deploy_dict['YAML'][YAML][site_id]
                else:
                    f.write(f"{'-':<8}")
            if m := re.search(r'DCB:(\d+):(\d+)', node_deploy_dict['FULL_INFO'][YAML]):
                f.write(f"(prefill:{m.group(1)}, decode:{m.group(2)})")
            f.write("\n")
        f.write("-"*90 + f"\n{'Total Nodes':<52}{final_totals['total']:<12}")
        for site_id in node_deploy_dict['SITE']:
            f.write(f"{final_totals[site_id]:<8}")
        f.write("\n")

    with open(f"{args.prefix_outfile}NODE_count_summary.by_site.txt", "w") as f:
        for site_id in node_deploy_dict['SITE']:
            f.write(f"### {re.sub(r'^fast-snova-ai-', '', site_id)} : total {sum(node_deploy_dict['SITE'][site_id].values())} {'-'*51}\n")
            for YAML in sorted(node_deploy_dict['SITE'][site_id], key=str.lower):
                yaml_file = f"{YAML}.yaml"
                f.write(f"{yaml_file:<52} : {node_deploy_dict['SITE'][site_id][YAML]}")
                if site_id in node_deploy_dict['PREFILL'][YAML]:
                    f.write(f"      (prefill:{node_deploy_dict['PREFILL'][YAML][site_id]}, decode:{node_deploy_dict['DECODE'][YAML][site_id]})")
                f.write("\n")
            f.write("\n")

    with open(f"{args.prefix_outfile}NODE_count_summary.by_model.txt", "w") as f:
        f.write(f"{'MODEL_ID':<40}{'SS':<20}{'BS':8}{'TOTAL':<8}")
        for site_id in node_deploy_dict['SITE']:
            f.write(f"{re.sub(r'^fast-snova-ai-', '', site_id):<8}")
        f.write("\n")
        for model in sorted(results, key=str.lower):
            first_model_line = 1
            f.write("-"*120 + "\n")
            for ss in sorted(results[model], key=sort_ss):
                first_ss_line = 1
                for batchsize in sorted(results[model][ss]['all_batch_sizes'], key=int):
                    yaml_files = results[model][ss]['all_batch_sizes'][batchsize]
                    total_nodes = sum(active_yaml_files[f] for f in yaml_files)
                    if first_model_line:
                        f.write(f"{model:<40}{ss:<20}{batchsize:<8}{total_nodes:<8}")
                    elif first_ss_line:
                        f.write(f"{'':<40}{ss:<20}{batchsize:<8}{total_nodes:<8}")
                    else:
                        f.write(f"{'':<40}{'':<20}{batchsize:<8}{total_nodes:<8}")
                    first_model_line = 0
                    first_ss_line = 0
                    for site_id in node_deploy_dict['SITE']:
                        site_total = 0
                        for YAML in yaml_files:
                            site_total += node_deploy_dict['SITE'][site_id][YAML] if YAML in node_deploy_dict['SITE'][site_id] else 0
                        if site_total == 0: site_total = '-'
                        f.write(f"{site_total:<8}")
                    # additional info:
                    yaml_files_str = ', '.join(list(f'{y}.yaml:{node_deploy_dict["FULL_INFO"][y]}' for y in yaml_files))
                    f.write(f"({yaml_files_str})\n")

    # print additional files : print_results_bundle_history(node_deploy_dict)
    # - NODE_deploy_date.by_bundle.txt
    # - NODE_deploy_date.by_bundle.git.log
    if args.full:
        print("Calling 'git log' to generate NODE_deploy_date.by_bundle.txt ...")
        pwd = os.getcwd()
        os.chdir(args.tfvars_dir)
        bundle_first_alive_dict = {}
        with open(f"{pwd}/{args.prefix_outfile}NODE_deploy_date.by_bundle.git.log", "w") as bundle_log:
            for bundle in sorted(active_yaml_files, key=str.lower):
                bundle_log.write(f'\n################## {bundle} ##################\n')
                bundle_first_alive_dict[bundle] = {}
                for site_id in node_deploy_dict['SITE']:
                    cmd = f'git log --date=format:"%m/%d/%Y %H:%M:%S" -S "^ +- name: {bundle}" --pickaxe-regex --follow {site_id}.tfvars'
                    output = subprocess.getoutput(cmd)
                    match = re.findall(r'Date:\s+(\S+)', output, re.DOTALL)
                    if match and site_id in node_deploy_dict['YAML'][bundle]:
                        bundle_first_alive_dict[bundle][site_id] = match[-1]
                    elif match:
                        bundle_first_alive_dict[bundle][site_id] = f"({match[-1]})"
                    else:
                        bundle_first_alive_dict[bundle][site_id] = "-"
                    bundle_log.write(f'# {cmd}\n{output}\n')

        with open(f"{pwd}/{args.prefix_outfile}NODE_deploy_date.by_bundle.txt", "w") as bundle_f:
            bundle_f.write(f"{'BUNDLE_NAME':<52}")
            for site_id in node_deploy_dict['SITE']:
                bundle_f.write(f"{re.sub(r'^fast-snova-ai-', '', site_id):<13}")
            bundle_f.write("\n" + "-"*90 + "\n")
            for bundle in sorted(active_yaml_files, key=str.lower):
                bundle_f.write(f"{bundle:<52}")
                for site_id in bundle_first_alive_dict[bundle]:
                    bundle_f.write(f"{bundle_first_alive_dict[bundle][site_id]:<13}")
                bundle_f.write("\n")

        os.chdir(pwd)
    else:
        pass

def main():
    args.tfvars_dir = re.sub(r'/$', '', args.tfvars_dir)
    default_tfvars = [
        f"{args.tfvars_dir}/fast-snova-ai-prod-0.tfvars",
        f"{args.tfvars_dir}/fast-snova-ai-prod-1.tfvars",
        f"{args.tfvars_dir}/fast-snova-ai-jp-prod-2.tfvars",
    ]
    if os.path.isfile(f"{args.tfvars_dir}"):
        default_tfvars = [args.tfvars_dir]
        args.tfvars_dir = os.path.abspath(os.path.dirname(args.tfvars_dir))

    # sanity check tfvar_files
    tfvar_files = []
    for tf in default_tfvars:
        if os.path.isfile(tf):
            print(f"Found: {tf} ...")
            tfvar_files.append(tf)

    # Extract results
    node_deploy_dict = extract_deployment_names(tfvar_files)
    results, PEF_ids = extract_pef_mapping(node_deploy_dict)
    
    # print results
    print_results_model_offering(results)
    print_results_PEF_summary(PEF_ids)
    print_results_yaml_summary(results, node_deploy_dict)

def analyze_yaml_files(in_files):
    nested_defaultdict = lambda: defaultdict(nested_defaultdict)
    results = nested_defaultdict()
    PEF_ids = defaultdict(dict)

    # read_yaml_file and build results dictionary
    if in_files[0].endswith(".flist"):
        yaml_files = subprocess.getoutput(f'cat {in_files[0]}').splitlines()
    else:
        yaml_files = in_files
    for yaml_file in yaml_files:
        try:
            with open(yaml_file, 'r') as f:
                docs = yaml.safe_load_all(f)
                for yaml_doc in docs:
                    if yaml_doc['kind'] == 'BundleTemplate':
                        read_bundle_depoly_yaml(yaml_file, yaml_doc, results, PEF_ids)
                    elif yaml_doc['kind'] == 'InferenceDeployment':
                        read_inference_depoly_yaml(yaml_file, yaml_doc, results, PEF_ids)
                    else:
                        pass
        except FileNotFoundError:
            print(f"Warning: {yaml_file} not found!")

    # print results
    print_results_model_offering(results, "bundle_snapshot.txt", "/dev/null", "/dev/null")

if __name__ == "__main__":
    args = parse_args()
    args.prefix_outfile = f"{args.prefix_outfile}." if args.prefix_outfile else ""
    if args.bundle:
        args.all_models_in_yaml = True
        analyze_yaml_files(args.bundle)
    else:
        main()
