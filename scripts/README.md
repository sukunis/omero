# RemoteImport:
Start import via OMERO.script for data stored at one of the supported workstations listed under ```WORKSTATION_NAMES```.
Source data are located under ```OMERO_ImportData/<username>/``` on the selected workstation. The username must be the same user name that is used for login in OMERO.
Appends non-image files with specified suffixes to project or dataset.

### Configurations:
```DEPTH```<br>
Set the number of directories to scan down for files during the import.

```DATA_PATH```<br>
Root storage location for data for inplace import.

```INPLACE_IMPORT```<br>
Specify if inplace import is used instead of normal import

```COPY_SOURCES```<br> 
Specify whether the sources should be copied to the server first before starting the import.
If the sources are to be copied first, it creates a directories in ```DATA_PATH``` like ```<username>_<userID>/yyyy-MM/dd/HH-mm-ss.SSS/```
and transfers the data before import starts.

```MOUNT_PATH```<br>
Main directory of mount point

```WORKSTATION_NAMES```<br>
mount point names/workstations

 
### Usage:

Select a ```PROJECT``` as target for your import :
A Dataset object is created for each subdirectory under ```OMERO_ImportData/<username>/```
The name of the dataset includes parent directory names. For example:
```/<mountpath>/dirA/dirB``` will result in datasetname: ```dirA_dirB```

Select a ```DATASET``` as target for your import :
All images (also images in subdirectories) are imported into this Dataset
