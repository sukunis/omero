# -*- coding: utf-8 -*-
"""
This script has been modified so it can be used
with OMERO version 5.2.x or newer.

This script gets the given Rectangle ROI from a particular image, then creates new
image with the regions within the ROIs for specified z and t, and saves them back to the server.

This script is adapted from [SCRIPTS]/omero/util_scripts/Images_From_ROIs.py
and New_Images_From_ROIs.py (See https://github.com/aherbert/omero-user-scripts)

@author Susanne Kunis
<a href="mailto:susebo@gmail.com">susebo@gmail.com</a>
@version 1.0
"""

import os
import time

import omero
import omero.scripts as scripts
from omero.gateway import BlitzGateway
from omero.rtypes import rstring, rlong

startTime = 0


def splitext(filename):
    """
    Splits a filename into base and extension.
    Handles .ome.tiff as an extension.
    """
    (base, ext) = os.path.splitext(filename)
    # Special case if a .ome.tif since only the .tif will be removed
    if base.endswith('.ome'):
        base = base.replace('.ome', '')
        ext = '.ome' + ext
    return (base, ext)


def createImageName(name, index):
    """
    Adds an ROI-index suffix to the source image names
    """
    name = os.path.basename(name)
    (base, ext) = splitext(name)
    return "%s_roi%d%s" % (base, index, ext)


def printDuration(output=True):
    global startTime
    if startTime == 0:
        startTime = time.time()
    if output:
        print "Script timer = %s secs" % (time.time() - startTime)


def getRectangles(conn, image,roi_id):
    """
    Returns a list of (x, y, width, height, zStart, zStop, tStart, tStop) of
    the given rectangle ROI roid_id in the image
    """

    rois = []

    roiService = conn.getRoiService()
    result = roiService.findByImage(image.id, None)
    
    for roi in result.rois:
        print "Check roi id"
        print roi.getId().getValue()
        if roi.getId().getValue() == roi_id:
            print "Equal id's"
            x = None
            theTs = []
            theZs = []
            for shape in roi.copyShapes():
                if type(shape) == omero.model.RectangleI:
                    # check t range and z range for every rectangle
                    if shape.getTheT() is not None:
                        theTs.append(shape.getTheT().val)
                    if shape.getTheZ() is not None:
                       theZs.append(shape.getTheZ().val)
                    if x is None:   # get x, y, width, height for first rect only
                        x = int(shape.getX().getValue())
                        y = int(shape.getY().getValue())
                        width = int(shape.getWidth().getValue())
                        height = int(shape.getHeight().getValue())

            # if we have found any rectangles at all...
            if x is not None:
                if len(theZs) > 0:
                    zStart = min(theZs)
                    zEnd = max(theZs)
                else:
                    zStart = 0
                    zEnd = image.getSizeZ() - 1
                if len(theTs) > 0:
                    tStart = min(theTs)
                    tEnd = max(theTs)
                else:
                    tStart = 0
                    tEnd = image.getSizeT() - 1
                rois.append((x, y, width, height, zStart, zEnd, tStart, tEnd))

    return rois


def processImage(conn, imageId, params):
    """
    Process an image.
    If "Entire stack" is True, we make a Z-stack and T-stack.
    Otherwise, we create a 5D image representing the ROI "cropping" the
    original image with T and Z like specified.
    Image is put in a dataset if specified.
    """

    createDataset = params['New_Dataset']
    datasetName = params['New_Dataset_Name']
    roi_id=params['ROI_ID']
    entire_stack=params['Entire_Stack']
    specified_zStack=params['OR Choose Z_Stack']

    z_start=0
    z_end=0
    if 'Z_Stack_Start' in params and 'Z_Stack_End' in params:
        z_start=params['Z_Stack_Start']
        z_end=params['Z_Stack_End']

    t_start=0
    t_end=0
    if 'Z_Stack_Start' in params and 'Z_Stack_End' in params:   
        t_start=params['T_Stack_Start']
        t_end =params['T_Stack_End']
        
    specified_tStack=params['OR Choose T_Stack']

    image = conn.getObject("Image", imageId)
    if image is None:
        return

    parentDataset = image.getParent()
    parentProject = parentDataset.getParent()

    dataset = None
    if not createDataset:
        dataset = parentDataset

    imageName = image.getName()
    updateService = conn.getUpdateService()

    pixels = image.getPrimaryPixels()
    W = image.getSizeX()
    H = image.getSizeY()

    # note pixel sizes (if available) to set for the new images
    physicalSizeX = pixels.getPhysicalSizeX()
    physicalSizeY = pixels.getPhysicalSizeY()
    physicalSizeZ = pixels.getPhysicalSizeZ()

    # Store original channel details
    cNames = []
    emWaves = []
    exWaves = []
    for index, c in enumerate(image.getChannels()):
        lc = c.getLogicalChannel()
        cNames.append(str(c.getLabel()))
        emWaves.append(lc.getEmissionWave())
        exWaves.append(lc.getExcitationWave())

    # x, y, w, h, zStart, zEnd, tStart, tEnd
    rois = getRectangles(conn, image,roi_id)
    print "rois"
    print rois

    # Make a new 5D image of ROI
    iIds = []
    for index, r in enumerate(rois):
        x, y, w, h, z1, z2, t1, t2 = r
        # Bounding box
        if x < 0:
            x = 0
        if y < 0:
            y = 0
        if x + w > W:
            w = W - x
        if y + h > H:
            h = H - y

        if entire_stack or specified_zStack == 'All Z planes':
            z1=0
            z2 = image.getSizeZ() - 1
        if entire_stack or specified_tStack == 'All T planes':
            t1=0
            t2 = image.getSizeT() - 1
       
        if specified_zStack == 'Other (see below)':
                z1 = z_start-1
                z2 = z_end-1
          
        if specified_tStack == 'Other (see below)':
                t1 = t_start-1
                t2 = t_end-1
           

        print "  ROI x: %s y: %s w: %s h: %s z1: %s z2: %s t1: %s t2: %s" % (
            x, y, w, h, z1, z2, t1, t2)

        # need a tile generator to get all the planes within the ROI
        sizeZ = z2-z1 + 1
        sizeT = t2-t1 + 1
        sizeC = image.getSizeC()
        zctTileList = []
        tile = (x, y, w, h)
        print "zctTileList..."
        for z in range(z1, z2+1):
            for c in range(sizeC):
                for t in range(t1, t2+1):
                    zctTileList.append((z, c, t, tile))

        def tileGen():
            for i, t in enumerate(pixels.getTiles(zctTileList)):
                yield t

        print "sizeZ, sizeC, sizeT", sizeZ, sizeC, sizeT
       
        description = """\
Created from Image ID: %d,
  Name: %s
  
  ROI ID: %d
  x: %d y: %d w: %d h: %d
  Selected Stack: Z[%d,%d], T[%d,%d]""" % (imageId, imageName, roi_id, x, y, w, h,z1+1,z2+1,t1+1,t2+1)
  
        # make sure that script_utils creates a NEW rawPixelsStore
        serviceFactory = conn.c.sf  # noqa
        newI = conn.createImageFromNumpySeq(
            tileGen(),createImageName(imageName, roi_id),
            #createImageName(imageName, index),
            sizeZ=sizeZ, sizeC=sizeC, sizeT=sizeT, description=description,
            dataset=dataset)
        iIds.append(newI.getId())

        # Apply colors from the original image to the new one
        if newI._prepareRenderingEngine():
            renderingEngine = newI._re

            # Apply the original channel names
            newPixels = renderingEngine.getPixels()

            for i, c in enumerate(newPixels.iterateChannels()):
                lc = c.getLogicalChannel()
                lc.setEmissionWave(emWaves[i])
                lc.setExcitationWave(exWaves[i])
                lc.setName(rstring(cNames[i]))
                updateService.saveObject(lc)

            renderingEngine.resetDefaultSettings(True)

        # Apply the original pixel size - Get the object again to refresh state
        newImg = conn.getObject("Image", newI.getId())
        newPixels = newImg.getPrimaryPixels()
        newPixels.setPhysicalSizeX(physicalSizeX)
        newPixels.setPhysicalSizeY(physicalSizeY)
        newPixels.setPhysicalSizeZ(physicalSizeZ)
        newPixels.save()

    if len(iIds) > 0 and createDataset:

        # create a new dataset for new images
        print "\nMaking Dataset '%s' of Images from ROIs of Image: %s" % (
            datasetName, imageId)
        dataset = omero.model.DatasetI()
        dataset.name = rstring(datasetName)
        desc = """\
Images in this Dataset are from ROIs of parent Image:
Name: %s
Image ID: %d""" % (imageName, imageId)
        dataset.description = rstring(desc)
        dataset = updateService.saveAndReturnObject(dataset)
        for iid in iIds:
            link = omero.model.DatasetImageLinkI()
            link.parent = omero.model.DatasetI(dataset.id.val, False)
            link.child = omero.model.ImageI(iid, False)
            updateService.saveObject(link)
        if parentProject:        # and put it in the current project
            link = omero.model.ProjectDatasetLinkI()
            link.parent = omero.model.ProjectI(parentProject.getId(), False)
            link.child = omero.model.DatasetI(dataset.id.val, False)
            updateService.saveAndReturnObject(link)

    return len(iIds)


def makeImagesFromRois(conn, params):
    """
    Processes the list of Image_IDs, either making a new image-stack or a new
    dataset from each image, with new image planes coming from the regions in
    Rectangular ROIs on the parent images.
    """

    
    ids = params["IDs"]

    count = 0
  
    for iId in ids:
        count += processImage(conn, iId, params)
   

    plural = (count == 1) and "." or "s."
    message = "Created %s new image%s" % (count, plural)
    if count > 0:
        message += " Refresh Project to view"
    return message


def runAsScript():
    """
    The main entry point of the script, as called by the client via the
    scripting service, passing the required parameters.
    """
    printDuration(False)    # start timer
    dataTypes = [rstring('Dataset'), rstring('Image')]
    defaultZOption='ROI position'
    zChoices=[rstring(defaultZOption),rstring('All Z planes'),rstring('Other (see below)')]
    defaultTOption='ROI position'
    tChoices=[rstring(defaultTOption),rstring('All T planes'),rstring('Other (see below)')]

    client = scripts.client('Crop_Image_From_Certain_ROI.py',
"""Create new Image from the region defined by Rectangle ROI ID.
Designed to work with multi-plane images.
""",

    #scripts.String("Data_Type", optional=False, grouping="1",
   #     description="Choose Images via their 'Dataset' or directly by "
    #                "'Image' IDs.",
    #    values=dataTypes, default="Image"),

   scripts.List("IDs", optional=False, grouping="1",
        description="Image ID to process."
        ).ofType(rlong(0)),

    scripts.Int("ROI_ID",optional=False,grouping="2",
        description="Choose rectangle ROI for cropping."
        ),

    scripts.Bool("Entire_Stack", grouping="3",
        description="Extend ROI through the entire stack (Z & T planes)",
        default=False),
                            
    scripts.String("OR Choose Z_Stack", grouping="4",
        description="Extend ROI through the specified Z-stacks.",values=zChoices,
        default=defaultZOption),

    scripts.Int("Z_Stack_Start",grouping="4.1",
        description="Choose a specific Z-index to export",min=1),
     scripts.Int("Z_Stack_End", grouping="4.2",
        description="Choose a specific Z-index to export",min=1),

    scripts.String("OR Choose T_Stack", grouping="5",
        description="Extend ROI through the specified T-stacks.",values=tChoices,
        default=defaultTOption),
    scripts.Int("T_Stack_Start", grouping="5.1",
        description="Choose a specific T-index to export",min=1),
     scripts.Int("T_Stack_End", grouping="5.2",
        description="Choose a specific T-index to export",min=1),

    scripts.Bool("New_Dataset", grouping="6",
        description="Create images in a new Dataset", default=False),
    scripts.String("New_Dataset_Name", grouping="6.1",
        description="New Dataset name", default="From_ROIs"),

    version="1.0",
    authors=["Susanne Kunis"],
    institutions=["University Osnabrueck"],
    contact="susebo@gmail.com",
    )  # noqa

    try:
        # process the list of args above.
        #parameterMap = {}
        #for key in client.getInputKeys():
        #    if client.getInput(key):
         #       parameterMap[key] = client.getInput(key, unwrap=True)

        #print parameterMap
        
        
        # create a wrapper so we can use the Blitz Gateway.
        conn = BlitzGateway(client_obj=client)

        scriptParams={}
        scriptParams=client.getInputs(unwrap=True)

        print scriptParams

        message = makeImagesFromRois(conn, scriptParams)

        if message:
            client.setOutput("Message", rstring(message))
        else:
            client.setOutput("Message",
                             rstring("Script Failed. See 'error' or 'info'"))

    finally:
        client.closeSession()
        printDuration()


if __name__ == "__main__":
    runAsScript()
