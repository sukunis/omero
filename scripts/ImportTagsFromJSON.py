import omero
import os
from omero.gateway import BlitzGateway
from omero.rtypes import rstring,unwrap,rlong,robject
import omero.scripts as scripts
import subprocess
import sys
import json
from omero_model_TagAnnotationI import TagAnnotationI
from omero_model_AnnotationAnnotationLinkI import AnnotationAnnotationLinkI

# omero.plugins.tag
omeroFiles_Path="/OMERO_dev/Files/"
SQL_Tag="""
        select a.id, a.description, a.textValue,
        a.details.owner.id, a.details.owner.firstName,
        a.details.owner.lastName
        from TagAnnotation a where a.id not in
        (select distinct l.child.id from AnnotationAnnotationLink l)
        and a.id not in
        (select distinct l.parent.id from AnnotationAnnotationLink l)
        and a.details.owner.id=:eid
        """
        
SQL_TagSet="""
    select distinct a.id, a.description, a.textValue
    from TagAnnotation a, AnnotationAnnotationLink b
    where a.id = b.parent.id
    and a.ns=:ns
    and a.details.owner.id=:eid
    """
    
SQL_TagSetChilds="""
    select a.id,a.description,a.textValue
    from TagAnnotation a
    where a.id in
    (select distinct b.child.id from AnnotationAnnotationLink b
    where b.parent.id=:pid)
    """

def getTagFile(conn,object_id,file_id):
    omero_object = conn.getObject("Project", int(object_id))
    if omero_object is None:
        sys.stderr.write("Error: Project does not exist.\n")
        sys.exit(1)
    file = None
    for ann in omero_object.listAnnotations():
        if isinstance(ann, omero.gateway.FileAnnotationWrapper):
            print("Load File ID: %d, %s Size: %d"%( ann.getFile().getId(), ann.getFile().getName(),
                                                     ann.getFile().getSize()))
            if (ann.getFile().getId() == int(file_id)):
                print ("Identify tag file")
                file = ann.getFile()._obj
    if file is None:
        sys.stderr.write("Error: File does not exist.\n")
        sys.exit(1)
    return file


#returns [false,None] if tag doens't exist yet, [true,tag ID] otherwise
def tagAvailable(client,conn,tagName):
    #print("check if tag %s is still available"%(tagName))
    
    params = omero.sys.ParametersI()
    params.addString('ns', omero.constants.metadata.NSINSIGHTTAGSET)
    params.map['eid']=rlong(long(conn.getUser().getId()))
    ice_map = dict()

    session = client.getSession()
    q = session.getQueryService()

    for element in q.projection(SQL_Tag, params, ice_map):
        tag_id, description, text, owner, first, last = map(unwrap, element)
        if tagName == text:
            return True,tag_id;
    #end for
    
    # search also in tagsets childs
    for element in q.projection(SQL_TagSet,params,ice_map):
        tagset_id, description, text = map(unwrap, element)
        params.map['pid']=rlong(long(tagset_id))
        for child in q.projection(SQL_TagSetChilds,params,ice_map):
            tag_id,tag_desc,tag_text=map(unwrap,child)
            if tagName == tag_text:
                return True,tag_id;
            #end if
        #end for
    #end for
    return False,None;

#returns [false,None] if tag doens't exist yet, [true,tag ID] otherwise
def tagSetAvailable(client,conn,tagSetName):
    #print("check if tagSet %s is still available"%(tagSetName))
    
    params = omero.sys.ParametersI()
    params.addString('ns', omero.constants.metadata.NSINSIGHTTAGSET)
    params.map['eid']=rlong(long(conn.getUser().getId()))
    ice_map = dict()

    session = client.getSession()
    q = session.getQueryService()

    for element in q.projection(SQL_TagSet, params, ice_map):
        tagSet_id, description, text = map(unwrap, element)
        if tagSetName == text:
            return True,tagSet_id;
    #end for
    return False,None;

def listTagSets(client,conn):
    print ("list available tags:")
    params = omero.sys.ParametersI()
    params.addString('ns', omero.constants.metadata.NSINSIGHTTAGSET)
    params.map['eid']=rlong(long(conn.getUser().getId()))
    ice_map = dict()

    session = client.getSession()
    q = session.getQueryService()

    # gives all tags are not in tagesets:
    for element in q.projection(SQL_Tag, params, ice_map):
        tag_id, description, text, owner, first, last = map(unwrap, element)
        print ("\tTag:",text)
    #end for

    
    for element in q.projection(SQL_TagSet,params,ice_map):
        tagset_id, description, text = map(unwrap, element)
        
        print ('\tTagset:',text)
        params.map['pid']=rlong(long(tagset_id))
      
        for child in q.projection(SQL_TagSetChilds,params,ice_map):
            tag_id,tag_desc,tag_text=map(unwrap,child)
            print ('\t\tChild',tag_text)
        # end for
     
    #end for
 

def load(conn,filepath):
    """
    Import new tag(s) from json.
    """
    if filepath:
        fobj = open(filepath,"r")
    else:
        sys.stderr.write("Error: No file is given.\n")
        sys.exit(1)
    #end ifelse

    p = json.load(fobj)


    if fobj is not sys.stdin:
        fobj.close()
    #end if

   

    update = conn.getUpdateService()
    tagList2=[]
    for tset in p:
        # {tag:<tagName>,desc:<description>}
        if "tag" in tset:
            tag=TagAnnotationI()
            tag.setTextValue(rstring(tset["tag"]))
            if tset["desc"]:
                tag.setDescription(rstring(tset["desc"]))
            #end if
            available,id = tagAvailable(client,conn,tset["tag"])
            if not available:
                print ("CREATE tag ",tset['tag'])
                tagList2.append(tag)
            else:
                print("DON'T CREATE: %s - Tag still available [%id] " % (tset["tag"],id))
            #end ifelse
        #end if

        if "tagset" in tset:
            childTagList=[]
            # get child tags
            #{tags:[{name:<tagName>,desc:<description>}],tagset:<tagSetName>,desc:<description>}
            for t in tset['tags']:
                try:
                    tag=TagAnnotationI()
    
                    tag.setTextValue(rstring(t["name"]))
                    if t["desc"]:
                        tag.setDescription(rstring(t["desc"]))
                    #end if
                    available,id = tagAvailable(client,conn,t["name"])
                    if not available:
                        print ("CREATE childtag ",t['name'])
                        childTagList.append(tag)
                    else:
                        print("DON'T CREATE: %s - ChildTag still available [%id] " % (t["name"],id))
                    #end ifelse
                except:
                    print ("ERROR: Can't parse ",t)
                #end try
                
            #end for
            childTagList=update.saveAndReturnArray(childTagList)

            # create tagset
            available,id = tagSetAvailable(client,conn,tset["tagset"])
            if not available:
                print ("CREATE tagSet ",tset['tagset'])
                tagSet=TagAnnotationI()
                tagSet.setTextValue(rstring(tset["tagset"]))
                if tset["desc"]:
                    tagSet.setDescription(rstring(tset["desc"]))
                #end if
                tagSet.setNs(rstring(omero.constants.metadata.NSINSIGHTTAGSET))
                tagSet = update.saveAndReturnObject(tagSet)
                links=[]
                for child in childTagList:
                    l=AnnotationAnnotationLinkI()
                    l.setChild(child)
                    l.setParent(tagSet)
                    links.append(l)
                #end for
                update.saveAndReturnArray(links)
            else:
                print("DON'T CREATE: %s - TagSet still available [%s] " % (tset["tagset"],id))
                tagSet=TagAnnotationI(id,False)
                links=[]
                for child in childTagList:
                    l=AnnotationAnnotationLinkI()
                    l.setChild(child)
                    l.setParent(tagSet)
                    links.append(l)
                #end for
                update.saveAndReturnArray(links)
                    
            #end ifelse
        #end if
    #end for
    update.saveAndReturnArray(tagList2)


def importTags(client,conn,script_params):
    obj_id=0 #long(script_params["ID"])
    file_id=0
    fileID=scriptParams["FileId"]
    #myPath=parseDirName(fileID)
    #p=subprocess.check_output("find /OMERO/Files/ -iname '"+fileID+"'", shell=True)
    #print "Output find ","["+p+"]"
#    myPath = [line[2:] for line in subprocess.check_output("find /OMERO/Files/ -iname '"+fileID+"'", shell=True).splitlines()]
    myPath = [line[0:] for line in subprocess.check_output("find /storage/OMERO/Files/ -iname '"+fileID+"'", shell=True).splitlines()]
    print ("Path: ",myPath)
    
    # define path to local json file
#    file_path=scriptParams["Filepath"]
    if myPath is None or len(myPath) == 0:
        sys.stderr.write("File doesn't exists! Maybe path to /storage/OMERO/Files is wrong?")
    else:
        if  os.path.isfile(myPath[0]):
            load(conn,myPath[0])
        else:
            sys.stderr.write("File doesn't exists: "+myPath[0])
            sys.exit(1)


if __name__ == "__main__":

    client = scripts.client(
        'ImportTagsFromJson.py',
        """
        Import Tag and TagSets from an uploaded *.json file.
        See also ExportTagsToJSON.
        """,
    scripts.String(
        "FileId",optional=False,grouping='01',description="See tooltip of annotation file to get the File ID"
    ),

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
        print (scriptParams)

        # wrap client to use the Blitz Gateway
        conn = BlitzGateway(client_obj=client)
        #list available tags
        listTagSets(client,conn)
        
        message = importTags(client, conn, scriptParams)
        client.setOutput("Message", rstring(message))

    finally:
        client.closeSession()
