
import omero.scripts as scripts
from omero.gateway import BlitzGateway
import os
import sys
import omero
import omero.cli
from omero.rtypes import rstring,unwrap,rlong,robject

import shlex
import subprocess
import time
import tempfile
from pathlib import Path
import threading
import datetime
import glob

############### CONFIGURATIONS ######################################
# Set the number of directories to scan down for files :
DEPTH= 10

MOUNT_PATH = "/Importer/"
WORKSTATION_NAME="cn-imaris"
WORKSTATION_NS="Imaris"
#####################################################################

PARAM_DATATYPE = "Data_Type"
PARAM_ID = "IDs"
PARAM_ATTACH = "Attach non image files"
PARAM_DEST_ATTACH = "Attach to object type"
PARAM_ATTACH_FILTER = "Filter attachment by extension"
PARAM_SKIP_EXISTING = "Skip already imported files"

IDLETIME = 5


def createDataset(conn,pID,dName):
    # create dataset for experiment dir
    dataset = omero.model.DatasetI()
    dataset.name = rstring(dName)
    dataset = conn.getUpdateService().saveAndReturnObject(dataset)
    # link to project
    link = omero.model.ProjectDatasetLinkI()
    link.setParent(omero.model.ProjectI(pID, False))
    link.setChild(dataset)
    conn.getUpdateService().saveObject(link)
    datasetID = dataset.getId().getValue()
    print("\t* Create Dataset with name %s -- ID: %d"%(dName,datasetID))

    return datasetID


def getImportTarget(conn,params):
    id = params.get(PARAM_ID)

    destID = None
    destType = None
    destObj = None

    if id is not None:
        destObj = conn.getObject(params.get(PARAM_DATATYPE), id)
        if destObj:
            destID = id
            destType = params.get(PARAM_DATATYPE)
    else:
        print("ERROR: please specify a target object like project or dataset!")

    if destType is None:
        print("ERROR: please specify a target object like project or dataset!")

    return destID,destObj,destType


def createArgumentList(ipath,id,skip,depth):
    import_args =["import"]
    import_args.extend(['-c']) # continue if errors
    import_args.extend(['-d',str(id)])
    import_args.extend(['--parallel-fileset','2'])
    import_args.extend(['--parallel-upload','2'])
    import_args.extend(['--no-upgrade-check'])
    import_args.extend(['--depth',str(depth)])
    if skip:
        import_args.extend(["--exclude=clientpath"])

    import_args.extend([Path(ipath).resolve().as_posix().replace("\ "," ")])

    return import_args




def parseLogFile(stderr):

    parseStr0 = "IMPORT_DONE Imported file:"
    parseStr1 = "ClientPath match for filename:"
    images_skipped=[]
    image_imported=[]
    index =0

    with open(stderr.name) as inFile:
        for line in inFile:
            if parseStr0 in line:
                p=(line[line.find(parseStr0)+len(parseStr0):len(line)]).strip()
                image_imported.append(p)
            if parseStr1 in line:
                p=(line[line.find(parseStr1)+len(parseStr1):len(line)]).strip()
                images_skipped.append(p)
            index=index+1

    return images_skipped,image_imported


# this function assume that minimum one image file was imported
# ATTENTION: doesn't work for unknown image file formats
# return
# 1. list of files for newly import (kind of this files was still imported)
# 2. list of other files (non image file format or not yet imported kind of file suffixes)
def validateImport(images_skipped,images_imported,ipath):

    # not_imported = image and non image file formats
    not_imported,suffixes = filterNotImported(images_skipped,images_imported,ipath)

    print("Not Imported: ",len(not_imported))
    print("Suffixes of nt imported: ",suffixes)

    newly_importList=[]
    other_fList=[]
    for f in not_imported:
        suffix = Path(f).suffix
        if suffix in suffixes:
            newly_importList.append("/"+f)
        else:
            other_fList.append(f)

    print ("Newly import files: ",len(newly_importList))
    print ("Other files: ",len(other_fList))

    return newly_importList,other_fList


# return
# 1. list of files that are not in the list of image_imported as well as in the skipped list,
#       but available in the imported source directory
# 2. list of imported or skipped file suffixes
def filterNotImported(images_skipped,image_imported,path):
    not_imported = []
    suffixes=[]

    filePaths=glob.iglob("%s/*.*"%path,recursive=False)

    for f in filePaths:
        if f.strip() not in image_imported:
            #print("\t [%s] is not in imported "%(f))
            if f.strip() not in images_skipped:
                #print("\t [%s] is not in skipped "%(f))
                not_imported.append(f)
            else:
                suffixes.append(Path(f).suffix)
        else:
            suffixes.append(Path(f).suffix)

    return not_imported,suffixes


def getFiles(pattern,dir,depth):
    result=[]

    if depth > 1:
        files=list(Path(dir).rglob(pattern))
    else:
        files=list(Path(dir).glob(pattern))
    for file in files:
        result.append(file.resolve().as_posix())

    if len(result)==0:
        return None

    return result


def attachFiles(conn, destID, destType,values,srcPath,namespace,depth):
    try:
        if len(values)==0:
            print ("\t WARN: No extension filter specified! No files will be attached.")
            return
        extFilters = values.split(",")
        destObj = None
        if destType == 'Dataset':
            destObj = conn.getObject("Dataset", destID)
        else:
            if destType == 'Project':
                destObj = conn.getObject("Dataset", destID).getParent()

        if destObj is None:
            print("ERROR attach files: can not attach files to object: None")
            return

        for extensionPattern in extFilters:
            extensionPattern.replace(" ", "")
            if len(extensionPattern) > 0:
                if not "." in extensionPattern:
                    extensionPattern = "." + extensionPattern
                if not "*" in extensionPattern:
                    extensionPattern = "*" + extensionPattern
                print("\t* attachments filter by pattern %s"%(extensionPattern))

                res=getFiles(extensionPattern,srcPath,depth)
                print("Found: ",list(res))
                if res is not None:
                    for attachFile in res:
                        print("\tATTACH File %s to %s "%(attachFile,str(destObj)))
                        if attachFile is not None:
                            file_ann = conn.createFileAnnfromLocalFile(attachFile, mimetype="text/plain",
                                                                       ns=namespace,
                                                                       desc=None)
                            #print "*** ATTACHING FileAnnotation to Dataset: ", "File ID:", file_ann.getId(), \
                            #      ",", file_ann.getFile().getName(), "Size:", file_ann.getFile().getSize()
                            destObj.linkAnnotation(file_ann)  # link to src file
                else:
                    print("\t=> no file found for pattern %s"% (extensionPattern))

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print('ERROR: attach file: %s\n %s %s'%(str(e),exc_type, exc_tb.tb_lineno))


def cliImport(client,ipath,destID,skip,depth,dataset=None,conn=None):
    # create import call string
    args = createArgumentList(ipath,destID,skip,depth)

    images_skipped = None
    images_imported = None

    try:
        with tempfile.NamedTemporaryFile(suffix=".stdout") as stdout:
            with tempfile.NamedTemporaryFile(suffix=".stderr") as stderr:
                cli = omero.cli.CLI()
                cli.loadplugins()
                #cli.set_client(client)
                cli.set_client(client.createClient(secure=True))
                args.extend(["--file", stdout.name])
                args.extend(["--errs", stderr.name])

                cli.invoke(args)
                images_skipped,images_imported=parseLogFile(stderr)
                print ("Images imported: ",len(images_imported))
                print ("Images skipped: ",len(images_skipped))

                if dataset:
                    #append log file
                    #link reportFile to object
                    ann = conn.createFileAnnfromLocalFile(
                        stderr.name, mimetype="text/csv",ns=WORKSTATION_NS+"_log" )
                    dataset.linkAnnotation(ann)

                return images_skipped,images_imported,stderr
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print ('ERROR at cli import: %s\n %s %s'%(str(e),exc_type, exc_tb.tb_lineno))
        return None,None,None
    finally:
        return images_skipped,images_imported,None




def retryImport(client, destinationID, filesForNewlyImport, images_skipped, numOfImportedFiles, skip):
    # retry failed imports
    not_imported_imgList = []
    messageRetry = ""
    retry=False
    if filesForNewlyImport is not None and len(filesForNewlyImport) > 0:
        retry = True
        messageRetry = "-----Retry Import For: " + str(len(filesForNewlyImport)) + " ------"
        print("---- Retry import-----")
        for f in filesForNewlyImport:
            print("Retry import for: ", f)
            messageRetry = messageRetry + "\n" + f
            r_images_skipped, r_images_imported,log = cliImport(client, f,destinationID,skip,1)

            # now the file should be imported or skipped
            if r_images_imported is not None and len(r_images_imported) > 0:
                numOfImportedFiles = numOfImportedFiles + 1
            elif r_images_skipped is not None and len(r_images_skipped) > 0:
                images_skipped.append(f)
            else: #failed
                not_imported_imgList.append(f)
    return messageRetry, not_imported_imgList,images_skipped,numOfImportedFiles, retry



def importContent(conn, params,jobs,namespace,depth):
    try:
        client = conn.c
        #see https://lists.openmicroscopy.org.uk/pipermail/ome-users/2014-September/004783.html
        router = client.getProperty("Ice.Default.Router")
        router = client.getCommunicator().stringToProxy(router)
        for endpoint in router.ice_getEndpoints():
            client.ic.getProperties().setProperty("omero.host",endpoint.getInfo().host)
            break
        else:
            raise Exception("no host configuration found")

        s = client.getSession()
        re = s.createRenderingEngine()

        #see also https://gist.github.com/jacques2020/ee863e83c3e2b663d68f
        class KeepAlive(threading.Thread):
            def run(self):
                self.stop = False
                while not self.stop:
                    time.sleep(IDLETIME)
                    try:
                        s.keepAllAlive([re])
                    except:
                        client.closeSession()
                        raise

        keepAlive = KeepAlive()
        keepAlive.start()
        time.sleep(IDLETIME * 2)

        all_skipped_img=[]
        all_notImported_img=[]

        for ipath in jobs:
            if ipath is not None:
                ipath = ipath.replace(" ", "\\ ")
                print("#--------------------------------------------------------------------\n")
                destID =jobs[ipath]
                destDataset = conn.getObject('Dataset', destID)

                if destDataset is not None:
                    skip=params.get(PARAM_SKIP_EXISTING)

                    # call import
                    print("\n Import files from : %s \n"%ipath)
                    images_skipped,images_imported,log=cliImport(client,ipath,destID,skip,depth,destDataset,conn)

                    # validate import
                    filesForNewlyImport,other_fList=validateImport(images_skipped,images_imported,ipath)

                    # attach files
                    if params.get(PARAM_ATTACH):
                        attachFiles(conn,destID,params.get(PARAM_DEST_ATTACH),
                                    params.get(PARAM_ATTACH_FILTER),ipath,namespace,depth)

                    messageRetry,not_imported_imgList,images_skipped,numOfImportedFiles, retry = retryImport(client, destID, filesForNewlyImport,
                                                                          images_skipped, len(images_imported),
                                                                          skip)

                    message="Imports Finished! "

                    all_notImported_img.extend(not_imported_imgList)
                    all_skipped_img.extend(images_skipped)

    # todo attach files in separates try catch
    except Exception as e: # work on python 3.x
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print ('ERROR: Failed to import: %s\n %s %s'%(str(e),exc_type, exc_tb.tb_lineno))
        return "ERROR"
    finally:
        keepAlive.stop = True
        if all_notImported_img is not None and len(all_notImported_img)>0:
            print(messageRetry)
            print("NOT IMPORTED FILES:")
            print('\n'.join(map(str, all_notImported_img)))
            errmessage="ATTENTION: there are failed imports (%d), please check the activity report" \
                       "or the dataset comment report"%(len(all_notImported_img))
            message = errmessage

        if not message:
            return "No imports!"
        return message




def existsAsChildOf(destObj,name):
    for dSet in destObj.listChildren():
        if dSet.getName() == name:
            print ("\t* Dataset directory still exists: ",name)
            return dSet.getId()
    return None


def scanSubdir(conn,currentdir,pName,jobs,destObj):
    # creation of new dataset if not exists
    if not currentdir.endswith(os.sep):
        currentdir = currentdir + os.sep
    dName = os.path.split(os.path.dirname(currentdir))[1]
    if pName:
        datasetName = "%s__%s"%(pName,dName)
    else:
        datasetName = dName

    existingID = existsAsChildOf(destObj,datasetName)

    if not existingID:
        existingID = createDataset(conn,destObj.getId(),datasetName)
    # dir append to joblist
    jobs[currentdir]=existingID

    # recursion for subdirs
    subdirs = filter(os.path.isdir, [os.path.join(currentdir, x) for x in os.listdir(currentdir)])
    for dir in subdirs:
        jobs = scanSubdir(conn,dir,datasetName,jobs,destObj)

    return jobs

# jobs={path_0:tID_0,...,path_N:tID_N}
def getJobsAndTargets(conn,datapath,destType,destID,destObj):
    '''Returns list of src paths and list of target object ids'''
    if destType=="Dataset":
        # only file paths
        jobs={}
        jobs[datapath] = destID
        return jobs,DEPTH
    else: # target = project
        # create common dataset for direct files under Omero_Importdir/<user>/
        datasetName = WORKSTATION_NS
        existingID = existsAsChildOf(destObj,datasetName)

        if not existingID:
            existingID = createDataset(conn,destObj.getId(),datasetName)

        jobs={}
        jobs[datapath]=existingID

        # create datasets like directories
        subdirs = filter(os.path.isdir, [os.path.join(datapath, x) for x in os.listdir(datapath)])
        for dir in subdirs:
            jobs = scanSubdir(conn,dir,None,jobs,destObj)

        return jobs,1

def remoteImport(conn,params,datapath,namespace):
    destID,destObj,destType=getImportTarget(conn,params)
    if destObj is None:
        return "ERROR: No correct target specified for import data!"

    if destType != "Dataset" and destType !="Project":
        return "ERROR:Please specify as target dataset or project!"

    startTime = time.time()

    jobs,depth = getJobsAndTargets(conn,datapath,destType,destID,destObj)
    for key in jobs:
        print(key, '->', jobs[key])

    message = importContent(conn, params,jobs,namespace,depth)

    endTime = time.time()
    print("Duration Import: ", str(endTime - startTime))

    return destObj, message



def checkWorkstation(conn,workstation_name,mnt_path,userName):
    try:
        srcPath = os.path.join(mnt_path,workstation_name)+os.sep
        print("Check mountpath: ",srcPath)
        if not os.path.isdir(srcPath):
            print('ERROR: Remote system not available: ',srcPath)
        else:
            print("==> available")

        serverPath = os.path.join(srcPath,userName) + os.sep
        print("Check userdir on mountpath: ",serverPath)
        if not os.path.isdir(serverPath):
            print('ERROR: No data available on remote system for user: ',userName)
        else:
            print("==> available")

        return serverPath
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print('ERROR: while reading mount dir: %s\n %s %s'%(str(e),exc_type, exc_tb.tb_lineno))

    return None

#####################################################################

def run_script():

    dataTypes = [rstring('Project'),rstring('Dataset')]
    # TODO: enable attaching to images
    dataTypes_attach = [rstring('Dataset'), rstring('Project')]

    """
    The main entry point of the script, as called by the client via the
    scripting service, passing the required parameters.
    """
    client = scripts.client(
        'Remote_Import_Imaris.py',
        """Remote import from Imaris Workstation:

        * Import the content of the OMERO_ImportData/<username>/ folder on the Imaris workstation.
        * Appends files with the specified suffix to the Project or Dataset.
        ---------------------------------------------------------------
        INPUT:
        ---------------------------------------------------------------
        Select PROJECT as TARGET for import : : A Dataset object is created for each subdirectory on OMERO_ImportData/<username>/

        Select DATASET as TARGET for import : : All images (also images in subdirectories) are imported into this Dataset


        """,
        scripts.String(PARAM_DATATYPE, optional=True, grouping="1",
                       description="Choose kind of destination object.",
                       values=dataTypes),
        scripts.Long(PARAM_ID, optional=True, grouping="2",
                     description="ID of destination object. Please select only ONE object."),
        scripts.Bool(PARAM_SKIP_EXISTING, grouping="3",
                     description="skip files that are already uploaded (checked 'import from' path).",
                     default=False),
        scripts.Bool(PARAM_ATTACH, grouping="4",
                     description="Attach containing non image files", default=False),
        scripts.String(PARAM_DEST_ATTACH, grouping="4.1",
                       description="Object to that should be attach",
                       values=dataTypes_attach, default="Dataset"),
        scripts.String(PARAM_ATTACH_FILTER, grouping="4.2",
                       description="Filter files by given file extension (for example txt, pdf). Separated by ','."),
        namespaces=[omero.constants.namespaces.NSDYNAMIC],
        version="1.0.0",
        authors=["Susanne Kunis", "CellNanOs"],
        institutions=["University of Osnabrueck"],
        contact="sukunis@uos.de",
    )  # noqa

    try:
        params = client.getInputs(unwrap=True)
        if os.path.exists(MOUNT_PATH):
            conn = BlitzGateway(client_obj=client)

            datapath=checkWorkstation(conn,WORKSTATION_NAME,MOUNT_PATH,conn.getUser().getName())
            if datapath:
                robj,message=remoteImport(conn,params,datapath,WORKSTATION_NS)

            client.setOutput("Message", rstring(message))
            if robj is not None:
                client.setOutput("Result", robject(robj._obj))
        else:
            client.setOutput("ERROR",rstring("No such Mount directory: %s"%MOUNT_PATH))
    finally:
        client.closeSession()


if __name__ == "__main__":
    run_script()
