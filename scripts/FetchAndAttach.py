#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Module Name: OMERO.script FetchAndAttach.py
Description: Grab the files from a selected workstation listed in WORKSTATION_NAMES, copy them to the in-place data directory DATA_PATH, and add them as attachments (symlink in OMERO_DATA_DIR =inplace attachment) to the selected OMERO object.
Prerequisite: Workstations are configured for automatic mounting on MOUNT_PATH for the OMERO server.
Author: Susanne Kunis <sinukesus@gmail.com>
Date: 2026-06-09
License: GPL v3
Version: 1.0.0
"""

import argparse
import omero.clients
import omero.cli
import sys
from omero_upload import upload_ln_s
from pathlib import Path
from omero.rtypes import rstring, robject
import omero.scripts as scripts
from omero.gateway import BlitzGateway
import datetime
import os
import shutil

########## CONFIG #####################################
MIMETYPE = 'zip/ptu'
NAMESPACE = 'uos/fetch_and_attach/'
OMERO_DATA_DIR = '/OMERO'
# Set the number of directories to scan down for files :
DEPTH = 10
# storage location for data for inplace import
DATA_PATH= "/storage/OMERO_inplace/users/"
# automount src path
MOUNT_PATH = "/Importer/"
# names of connected workstation devices
WORKSTATION_NAMES = ["workstation1","workstation2"]
########################################################

PARAM_WS = "Workstations"
PARAM_DATATYPE = "Data_Type"
PARAM_ID = "IDs"
PARAM_ATTACH_FILTER = "Filter attachment by extension"

IDLETIME = 5

def get_formated_date():
    dt = datetime.datetime.now()
    year_m=dt.strftime("%Y-%m")
    day=dt.strftime("%d")
    time=dt.strftime("%H-%M-%S.%f")[:-3]
    return year_m,day,time

# creates directories: <username>_<userID>/yyyy-MM/dd/HH-mm-ss.SSS/
def create_new_repo_path(userName: str, userID: int):
    p1 = "%s_%s"%(userName,userID)
    p2,p3,p4 = get_formated_date()
    repo_path = os.path.join(DATA_PATH, os.path.join(os.path.join(os.path.join(p1, p2), p3), p4))
    os.makedirs(repo_path, exist_ok=True)
    return repo_path


def attach_data(conn, target: str, file: str, namespace: str, mimetype: str):
    path = Path(file)
    filename = path.name
    target_type = target.split(":")[0]
    target_id = target.split(":")[1]
    tmp = list(conn.getObjects(target_type, attributes={"id": target_id}))

    if len(tmp) == 0:
        sys.exit("Target not found")
    if len(tmp) > 1:
        sys.exit("More than one target found")
    tgt = tmp[0]

    existingfas = set(
        a.getFile().name for a in tgt.listAnnotations()
        if isinstance(a, omero.gateway.FileAnnotationWrapper))
    if filename in existingfas:
        #sys.exit("File already attached.")
        print(f"WARN: File already attached: {path.resolve()}")
        return None

    print("Attaching {} to {} {} [{}]".format(
        path.resolve(), target_type, tgt.getName(), tgt.getId()))

    fa = omero.model.FileAnnotationI()
    fo = upload_ln_s(conn.c, path.resolve(), OMERO_DATA_DIR, mimetype)
    fa.setFile(fo._obj)
    fa.setNs(omero.rtypes.rstring(namespace))
    fa = conn.getUpdateService().saveAndReturnObject(fa)
    fa = omero.gateway.FileAnnotationWrapper(conn, fa)

    tgt.linkAnnotation(fa)
    return fa
    
    
def get_files(filter_list: list, folder: str) -> list[str]:
    max_depth = DEPTH
    results = []
    extensions = None

    if filter_list:
        extensions = {ext if ext.startswith('.') else f'.{ext}' for ext in filter_list}

    for root, dirs, files in os.walk(folder):
        # Tiefenberechnung
        depth = root.replace(folder, '').count(os.sep)
        if depth > max_depth:
            dirs.clear()  # keine weiteren Unterordner
            continue
        
        for file in files:
            if extensions is None or os.path.splitext(file)[1].lower() in extensions:
                results.append(os.path.join(root, file))

    return sorted(results)



def identify_data(datapath: str, filter_list: list) -> list[str]:
    if not filter_list or len(filter_list) == 0:
        print("\t WARN: No extension filter specified! Attach all files")
        file_list = get_files(None,datapath)
        return file_list
    
    extFilters = filter_list.split(",")
    clean_list = []
    for extensionPattern in extFilters:
            extensionPattern.replace(" ", "")
            if len(extensionPattern) > 0:
                if not "." in extensionPattern:
                    extensionPattern = "." + extensionPattern
                
                print("\t* attachments filter by pattern %s"%(extensionPattern))
                clean_list.append(extensionPattern)

    file_list = get_files(clean_list,datapath)
    
    return file_list



def check_workstations() -> list:
    result = []
    for w in WORKSTATION_NAMES:
        srcPath = os.path.join(MOUNT_PATH,w)+os.sep
        
        if not os.path.isdir(srcPath):
            result.append(f"{w} offline")
        else:
            result.append(w)
    return result
            

def check_workstation_is_online(ws):
    if not os.path.isdir(os.path.join(MOUNT_PATH,ws)+os.sep):
        return False
    else:
        return True



def check_data_path(srcPath, userName):
    try:
        print("Mountpath: ", srcPath)
        dataPath = os.path.join(srcPath, userName) + os.sep
        
        if not os.path.isdir(dataPath):
            sys.exit(f'ERROR: No data available on remote system for user: {userName}')
            return None
    
        print("Datadir on mountpath: ",dataPath)
        return dataPath
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        sys.exit(f'ERROR: while reading mount dir: {str(e)}\n {exc_type} {exc_tb.tb_lineno}')
    return None

def copy_data(file_list: list, source_folder: str, target_folder: str) -> list[str]:
    target = Path(target_folder)
    source = Path(source_folder)
    path_list = []

    for file_path in file_list:
        src = Path(file_path)
        
        # Relative Pfad berechnen
        try:
            relative = src.relative_to(source)
        except ValueError:
            relative = src.name
        
        dst = Path.joinpath(target, relative)
        dst.parent.mkdir(parents=True, exist_ok=True)
        
        shutil.copy2(src, dst)
        path_list.append(dst)
  
    return path_list



def run_script():
    dataTypes = [rstring('Dataset'),rstring('Project')]
    client = scripts.client(
        'Fetch_and_attach_data.py',
        """
        This will grab files from your user folder on OMERO_ImportData/ on the chosen workstation and add them to the selected OMERO object.
        ---

        **NOTE**: Please ensure the workstation is turned on and connected to the network before starting.
        ---
        """,
        scripts.String(PARAM_WS, optional=False, grouping="1",
                       description="Choose a workstation that is NOT offline where you want to import from",
                       values=WORKSTATION_NAMES),
        scripts.String(PARAM_DATATYPE, optional=True, grouping="2",
                       description="Choose kind of destination object.",
                       values=dataTypes),
        scripts.Long(PARAM_ID, optional=False, grouping="3",
                     description="ID of destination object. Please select only ONE object."),
        scripts.String(PARAM_ATTACH_FILTER, grouping="4",
                       description="Filter files by given file extension (for example txt, pdf). Separated by ','."),
        namespaces=[omero.constants.namespaces.NSDYNAMIC],
        version="1.0.0",
        authors=["Susanne Kunis", "CellNanOs"],
        institutions=["University of Osnabrueck"],
        contact="sinukesus@gmail.com",
    )

    try:
        params = client.getInputs(unwrap=True)
        if os.path.exists(MOUNT_PATH):
            ws = params.get(PARAM_WS)
            if check_workstation_is_online(ws):
                conn = BlitzGateway(client_obj=client)
                data_src_path = check_data_path(os.path.join(MOUNT_PATH,ws)+os.sep,conn.getUser().getName())
                if data_src_path:
                    filter_list = params.get(PARAM_ATTACH_FILTER)
                    data_list = identify_data(data_src_path, filter_list)
                    dest = create_new_repo_path(conn.getUser().getName(), conn.getUser().getId())
                    print("Copy data to: ",dest)
                    data_path_list = copy_data(data_list, data_src_path, dest)
                    target = f"{params.get(PARAM_DATATYPE)}:{params.get(PARAM_ID)}"
                    robj = None
                    message = "Data attached"
                    for file in data_path_list:
                        namespace = f"{NAMESPACE}{params.get(PARAM_WS)}/"
                        robj = attach_data(conn, target, file, namespace, MIMETYPE)
                        if not robj:
                            message = "Not all files have been attached. Please check the LOG file (i)!"

                else:
                    message = "No data available on %s for user"%(ws)
                    robj = None

                client.setOutput("Message", rstring(message))
                if robj is not None:
                    client.setOutput("File_Annotation", robject(robj._obj))

            else:
                client.setOutput("ERROR", rstring(f"{ws} is offline!"))
                client.closeSession()
                sys.exit(1)
        else:
            client.setOutput("ERROR",rstring("No such Mount directory: %s"%MOUNT_PATH))
            client.closeSession()
            sys.exit(1)
    finally:
        client.closeSession()



if __name__ == '__main__':
    run_script()

    """ parser = argparse.ArgumentParser()
    parser.add_argument("file", help="The file to attach.")
    parser.add_argument("target", help="The target, e.g. Screen:1234")
    parser.add_argument(
        "-m", "--mimetype", default=MIMETYPE,
        help="Mimetype (default: text/csv)")
    parser.add_argument(
        "-ns", "--namespace", default=NAMESPACE,
        help="Namespace (default: openmicroscopy.org/idr/analysis/original)")
    parser.add_argument(
        "-u", "--upload", action="store_true",
        help="Upload the file (default: 'ln -s' into managed repository)")
    parser.add_argument(
        "-n", "--name", action="store_true",
        help="Use target name (e.g."
        " Project:idr0111-lee-cellmigration/experimentA) (default: id)")

    args = parser.parse_args()

    with omero.cli.cli_login() as c:
        conn = omero.gateway.BlitzGateway(client_obj=c.get_client())
        main(conn, args) """
