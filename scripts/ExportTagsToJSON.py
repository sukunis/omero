# -*- coding: utf-8 -*-
"""
This script can be used with OMERO version 5.2.x or newer.

This script dump all tags to an JSON-file and the tags can be reloaded by ImportTagsFromJSON.py.

@author Susanne Kunis
<a href="mailto:susebo@gmail.com">susebo@gmail.com</a>
@version 1.0
"""
import omero
from omero.gateway import BlitzGateway 
from omero.rtypes import rstring,unwrap,rlong,robject
import omero.scripts as scripts
import sys
import json
# from tempfile import NamedTemporaryFile,TemporaryFile,mkdtemp
from datetime import datetime
import os

def listTagSets(client,conn):
    params = omero.sys.ParametersI()
    params.addString('ns', omero.constants.metadata.NSINSIGHTTAGSET)
    
    user = conn.getUser()
    params.map['eid']=rlong(long(user.getId()))
    ice_map = dict()

    session = client.getSession()
    q = session.getQueryService()


    jsonString=[]    

    # gives all tags are not in tagesets:
    sql = """
        select a.id, a.description, a.textValue,
        a.details.owner.id, a.details.owner.firstName,
        a.details.owner.lastName
        from TagAnnotation a where a.id not in
        (select distinct l.child.id from AnnotationAnnotationLink l)
        and a.id not in
        (select distinct l.parent.id from AnnotationAnnotationLink l)
        and a.details.owner.id=:eid
        """

    for element in q.projection(sql, params, ice_map):
        tag_id, description, text, owner, first, last = map(unwrap, element)
        #print "Tag:",text
        jsonString.append({'tag':text,'desc':description})
    #end for


    #print "list tagsets"
    sql="""
    select distinct a.id, a.description, a.textValue
    from TagAnnotation a, AnnotationAnnotationLink b
    where a.id = b.parent.id
    and a.ns=:ns
    and a.details.owner.id=:eid
    """

    sql2="""
    select a.id,a.description,a.textValue
    from TagAnnotation a
    where a.id in
    (select distinct b.child.id from AnnotationAnnotationLink b
    where b.parent.id=:pid)
    """


    
    for element in q.projection(sql,params,ice_map):
        tagset_id, description, text = map(unwrap, element)
        tagsetString=text+'\n'
        #print 'Tagset:',tagsetString
        params.map['pid']=rlong(long(tagset_id))
        jsonElemChilds=[]
        for child in q.projection(sql2,params,ice_map):
            tag_id,tag_desc,tag_text=map(unwrap,child)
            jsonElemChild={'name':tag_text,'desc':tag_desc}
            jsonElemChilds.append(jsonElemChild)
            # print 'Child',tag_text
        # end for
        jsonElem={
            'tagset':text,
            'desc':description,
            'tags':jsonElemChilds
        }
        jsonString.append(jsonElem)
    #end for
   # print 'JSONString',jsonString


    
    return jsonString 


def addJSONFile(TXT,conn,client):
    """ Add a simple text file into the zip to explain what's there """

    n = datetime.now()
    # time-stamp name by default: Figure_2013-10-29_22-43-53.pdf
    figureName = '/tmp/export_tags_%s-%s-%s_%s-%s-%s.json' % (n.year, n.month, n.day, n.hour, n.minute, n.second)
    ns='omero.gateway.export_tags'

    fileAnn=None
    try: 
        #tempFile=NamedTemporaryFile()
        tempFile=open(figureName,'w')
        json.dump(TXT,tempFile)
        tempFile.flush()
        fileAnn=conn.createFileAnnfromLocalFile(tempFile.name,mimetype="text/plain",ns=ns)
        if fileAnn is not None:
            client.setOutput("File_Annotation",robject(fileAnn._obj))
        else:
            client.setOutput("Message",rstring("no file available for download"))
    finally:
        tempFile.close
        os.remove(figureName)

def exportTags(client,conn):
    message = listTagSets(client,conn)
    fileAnn=addJSONFile(message,conn,client)
    
#add annotation
    return fileAnn


if __name__ == "__main__":

    client = scripts.client(
        'ExportTagsToJSON.py',
        """
        Dump all Tags and TagSets of current user in selected group to *.json file for download.
        JSON-Format:
        [
            {"tag":"Aaa00", "desc":"Aaa00 desc"},
            {
                "tagset":"Aaa01", 
                "desc":"Aaa01 description",
                "tags":[{"name":"Aaa01_sub1","desc":"Aaa01_sub1 desc"}]
            }
        ]
        """,


        version="1.0",
        authors=["Susanne Kunis"],
        institutions=["University Osnabrueck"],
        contact="susanne.kunis@biologie.uni-osnabrueck.de",
    )

    try:

        # process the list of args above.
        scriptParams = {}
        # for key in client.getInputKeys():
        #     if client.getInput(key):
        #         scriptParams[key] = client.getInput(key, unwrap=True)
        # print scriptParams

        # wrap client to use the Blitz Gateway
        conn = BlitzGateway(client_obj=client)
        fileAnnotation = exportTags(client,conn)
        client.setOutput("Message",rstring("Json file created"))

    finally:
        client.closeSession()
