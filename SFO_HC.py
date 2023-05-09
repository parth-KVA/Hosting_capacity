# import library
import opendssdirect as dss
import os
import pandas as pd
import requests
import math
import numpy as np
import time

import networkx as nx
import matplotlib.pyplot as plt
import matplotlib as mlib

import boto3
from botocore import UNSIGNED
from botocore.client import Config
from pathlib import Path

s3_conn = boto3.client('s3', config = Config(signature_version=UNSIGNED))
NREL_bucket = 'oedi-data-lake'
#paginator = s3_conn.get_paginator('list_objects')
#result = paginator.paginate(Bucket=NREL_bucket, Delimiter='/')
#for prefix in result.search('CommonPrefixes'):
#    print(prefix.get('Prefix'))
SMART_DS_prefix = 'SMART-DS%2Fv1.0%2F2018%2FAUS%2FP2U%2Fscenarios%2Fsolar_high_batteries_none_timeseries%2Fopendss_no_loadshapes%2Fp2uhs0_1247%2Fp2uhs0_1247--p2udt1587%2F'
SMART_DS_prefix_network = 'SMART-DS/v1.0/2018/AUS/P2U/scenarios/solar_high_batteries_none_timeseries/opendss_no_loadshapes/p2uhs0_1247/p2uhs0_1247--p2udt1587/'
SMART_DS_prefix_load = 'SMART-DS/v1.0/2018/AUS/P2U/load_data/'
SMART_DS_prefix_PV = 'SMART-DS/v1.0/2018/AUS/P2U/solar_data/'


#s3_result = s3_conn.list_objects_v2(Bucket = NREL_bucket, Prefix = SMART_DS_prefix_e, Delimiter = '/')
#s3_conn.download_file(NREL_bucket+'/'+ SMART_DS_prefix_e,'Intermediates.txt', 'Intermediates.txt')
#print(open('Intermediates.txt').read())
#print (s3_result)
#print(s3_result.objects.all())
#for o in s3_result.get('CommonPrefixes'):
#    print('sub folder : ', o.get('Prefix'))
#for my_bucket_object in s3_result.objects.all():
#    print(my_bucket_object.key)
#print (s3_result)
#file_list = []
#for key in s3_result.get('Contents'):
#    file_list.append(key['Key'])
#print(f"List count = {len(file_list)}")


#'https://data.openei.org/s3_viewer?bucket=oedi-data-lake&limit=100&prefix=SMART-DS/v1.0/2018/AUS/P2U/scenarios/solar_high_batteries_none_timeseries/opendss_no_loadshapes/p2uhs0_1247/p2uhs0_1247--p2udt1587/'

#for s3_object in Opendss_bucket.objects.all():
    # Need to split s3_object.key into path and file name, else it will give error file not found.
#    path, filename = os.path.split(s3_object.key)
#    Opendss_bucket.download_file(s3_object.key, filename)

#for my_bucket_object in Opendss_bucket.objects.all():
#    print(my_bucket_object)


def get_file_folders(s3_client, bucket_name, prefix=""):
    file_names = []
    folders = []

    default_kwargs = {
        "Bucket": bucket_name,
        "Prefix": prefix
    }
    next_token = ""

    while next_token is not None:
        updated_kwargs = default_kwargs.copy()
        if next_token != "":
            updated_kwargs["ContinuationToken"] = next_token

        response = s3_client.list_objects_v2(**default_kwargs)
        contents = response.get("Contents")

        for result in contents:
            key = result.get("Key")
            if key[-1] == "/":
                folders.append(key)
            else:
                file_names.append(key)

        next_token = response.get("NextContinuationToken")

    return file_names, folders

def download_files(s3_client, bucket_name, local_path, file_names, folders):

    local_path = Path(local_path)
    for folder in folders:
        folder_path = Path.joinpath(local_path, folder)
        folder_path.mkdir(parents=True, exist_ok=True)

    for file_name in file_names:
        file_path = Path.joinpath(local_path, file_name)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        s3_client.download_file(
            bucket_name,
            file_name,
            str(file_path)
        )
def initiate_file_download(local_path, file_names, folders):

    download_files(
        s3_conn,
        NREL_bucket,
        local_path,
        file_names,
        folders
    )

def find_master_DSS(file_names, suffix):
    for file in file_names:
        #print (file)
        if file.endswith(suffix):
            return file 

# Read the Loads.dss file for the feeder. Copied from the NREL SMART_DS code
def load_loadfile(load_file):
    load_profile_map = {}
    with open(load_file) as f_load:
        for row in f_load.readlines():
            sp = row.split()
            name=None
            profile = None
            multiplier = 1
            for token in sp:
                if token.startswith('Load.'):
                    name = token.split('.')[1]
                    if name.endswith('_1') or name.endswith('_2'):
                        multiplier = 0.5 # Center-tap loads
                if token.startswith('!yearly'):
                    profile_raw = token.split('=')[1]
                    if 'mesh' in profile_raw:
                        profile = 'mesh'
                    else:
                        profile_sp = profile_raw.split('_')
                        profile = profile_sp[0]+'_'+profile_sp[2]+'.parquet'
                if profile is not None:
                    load_profile_map[name] = (profile,multiplier)
    return load_profile_map

#Read the PVSystems.dss file for the feeder
def load_PVfile(pv_file):
    pv_profile_map = {}
    with open(pv_file) as f_pv:
        for row in f_pv.readlines():
            #print (row)
            sp = row.split()
            name=None
            profile = None
            for token in sp:
                if token.startswith('PVSystem.'):
                    name = token.split('.')[1]
                if token.startswith('!yearly'):
                    profile = token.split('=')[1]
                if profile is not None:
                    pv_profile_map[name] = profile+'_full.csv'
    return pv_profile_map

def dss_run_command(local_path, Master_DSS_file):

    
    dss.run_command("Clear")
    dss.Basic.ClearAll()
    sol = dss.Solution

    local_path = os.path.join(r''+local_path)
    master = os.path.join(local_path, Master_DSS_file)

    #feeder_path = os.path.join(r''+local_path+'/'+Master_DSS_file)
    #master_file_name = 'Master.dss'
    #master = os.path.join(feeder_path, master_file_name)
    #dss.run_command(f'redirect {master}')
    #dss.run_command(f'redirect {master}')
    #dss.Solution.Convergence(0.0001)

    result = dss.run_command("Redirect " + master)
    print(result,flush=True)
    #DSScircuit = dss.Circuit

    #for i in DSScircuit.AllBusNames():
    #    print(i)

    #dss_bus_data = get_bus_data(DSScircuit)
 
    #va, voltage_dict, xfmr_loading_dict, line_loading_dict = simulate_feeder_condition(feeder_path)


    def loads():
        dss.Loads.First()
        while True:
            name = dss.Loads.Name()
            multiplier = 1
            
            parquet_name = load_profile_map[name][0]
            multiplier = load_profile_map[name][1] # For center-tap loads
            parquet_data = pd.read_parquet(os.path.join(load_data_folder,parquet_name))
            kw = parquet_data.loc[peak_timepoint, 'total_site_electricity_kw'] * multiplier
            kvar = parquet_data.loc[peak_timepoint, 'total_site_electricity_kvar'] * multiplier
 
            dss.Loads.kW(kw)
            dss.Loads.kvar(kvar)
            
            if not dss.Loads.Next() > 0:
                break
    
    def PV():
        first_pv = dss.PVsystems.First()
        while True and first_pv > 0: # Check that there is a PVsystems object on the network
            name = dss.PVsystems.Name()
            csv_name = pv_profile_map[name]
            csv_data = pd.read_csv(os.path.join(solar_data_folder,csv_name),header=0)
            irradiance = csv_data.loc[peak_timepoint,'PoA Irradiance (W/m^2)']
            res1 = dss.PVsystems.Irradiance(irradiance/1000)
 
            if not dss.PVsystems.Next() > 0:
                break
    
    

if __name__ == "__main__":

    dir_path = os.path.dirname(os.path.realpath(__file__))
    file_names, folders = get_file_folders(s3_conn, NREL_bucket, SMART_DS_prefix_network)
    
    suffix_master = "Master.dss"
    suffix_load = "Loads.dss"
    suffix_PV = "PVSystems.dss"
    Master_DSS_file = find_master_DSS(file_names, suffix_master)
    Load_DSS_file = find_master_DSS(file_names, suffix_load)
    PV_DSS_file = find_master_DSS(file_names, suffix_PV)
    
    pv_profile_map = load_PVfile(dir_path + '/' + PV_DSS_file)
    load_profile_map = load_loadfile(dir_path + '/' + Load_DSS_file)
    
    dss_run_command(dir_path,Master_DSS_file)

