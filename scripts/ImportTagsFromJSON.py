# -*- coding: utf-8 -*-
"""
This script can be used with OMERO version 5.2.x or newer.

This script import tags by an JSON-file created by ExportTagsToJSON.py

Attention: Please adapt parseDirName to your system

@author Susanne Kunis
<a href="mailto:susebo@gmail.com">susebo@gmail.com</a>
@version 1.0
"""
import omero
import os 
from omero.gateway import BlitzGateway
from omero.rtypes import rstring,rlong
import omero.scripts as scripts
import subprocess
import sys
import json
from omero_model_TagAnnotationI import TagAnnotationI
from omero_model_AnnotationAnnotationLinkI import AnnotationAnnotationLinkI

# omero.plugins.tag

def getTagFile(conn,object_id,file_id):
    omero_object = conn.getObject("Project", int(object_id))
    if omero_object is None:
        sys.stderr.write("Error: Project does not exist.\n")
        sys.exit(1)
    file = None
    for ann in omero_object.listAnnotations():
        if isinstance(ann, omero.gateway.FileAnnotationWrapper):
            print "Load File ID:", ann.getFile().getId(), ann.getFile().getName(),\
                "Size:", ann.getFile().getSize()
            if (ann.getFile().getId() == int(file_id)):
                print "Identify tag file"
                file = ann.getFile()._obj
    if file is None:
        sys.stderr.write("Error: File does not exist.\n")
        sys.exit(1)
    return file



def load(conn,filepath):
    """
    Import new tag(s) from json.
    """
    if filepath:
        fobj = open(filepath,"r")
    else:
        sys.stderr.write("Error: No file is given.\n")
        sys.exit(1)

    p = json.load(fobj)


    if fobj is not sys.stdin:
        fobj.close()


    update = conn.getUpdateService()
    tagList2=[]
    for tset in p:
        if "tag" in tset:
            tag=TagAnnotationI()
            tag.setTextValue(rstring(tset["tag"]))
            if tset["desc"]:
                tag.setDescription(rstring(tset["desc"]))
            tagList2.append(tag)
        #end if

        if "tagset" in tset:
            tagList=[]
            for t in tset['tags']:
                tag=TagAnnotationI()

                tag.setTextValue(rstring(t["name"]))
                if t["desc"]:
                    tag.setDescription(rstring(t["desc"]))
                tagList.append(tag)
            #end for
            tagList=update.saveAndReturnArray(tagList)

            tag=TagAnnotationI()
            tag.setTextValue(rstring(tset["tagset"]))
            if tset["desc"]:
                tag.setDescription(rstring(tset["desc"]))
            tag.setNs(rstring(omero.constants.metadata.NSINSIGHTTAGSET))
            tag = update.saveAndReturnObject(tag)
            links=[]
            for child in tagList:
                l=AnnotationAnnotationLinkI()
                l.setChild(child)
                l.setParent(tag)
                links.append(l)
            #end for
            update.saveAndReturnArray(links)
        #end if
    #end for
    update.saveAndReturnArray(tagList2)

def parseDirName(fileIdString):
    fileIdNum=int(fileIdString)
    if fileIdNum<1000:
        return "/data/omero/Files/Dir-000/"+fileIdString
    elif fileIdNum < 10000:
        return "/data/omero/Files/Dir-00"+fileIdString[:1]+"/"+fileIdString
    elif fileIdNum<100000:
        return "/data/omero/Files/Dir-0"+fileIdString[:2]+"/"+fileIdString
    else:
        return "/data/omero/Files/Dir-"+fielIdString[:3]+"/"+fileIdString



def importTags(client,conn,script_params):
    obj_id=0 #long(script_params["ID"])
    file_id=0
    fileID=scriptParams["FileId"]
    myPath=parseDirName(fileID)
#    file_path=scriptParams["Filepath"]
    if not os.path.exists(myPath):
        sys.stderr.write("Path doesn't exists: "+myPath)
        sys.exit(1)
    else:
        load(conn,myPath)





if __name__ == "__main__":

    client = scripts.client(
        'ImportTagsFromJson.py',
        """
        Import Tag and TagSets from an uploaded *.json file.
        See also ExportTagsToJSON.
        """,
    scripts.String(
        "FileId",optional=False,grouping='01',description="See tooltip of annotation file to get the File ID"),

        version="1.0",
        authors=["Susanne Kunis"],
        institutions=["University Osnabrueck"],
        contact="susanne.kunis@biologie.uni-osnabrueck.de",
    )

    try:

        # process the list of args above.
        scriptParams = {}
        for key in client.getInputKeys():
            if client.getInput(key):
                scriptParams[key] = client.getInput(key, unwrap=True)
        print scriptParams

        # wrap client to use the Blitz Gateway
        conn = BlitzGateway(client_obj=client)
        message = importTags(client, conn, scriptParams)
        client.setOutput("Message", rstring(message))

    finally:
        client.closeSession()
