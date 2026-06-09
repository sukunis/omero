# OMERO Remote Import

A sophisticated server-side OMERO script designed to import image data from networked workstations directly into OMERO Projects or Datasets, with optional support for in-place imports and non-image file attachments.

## 🚀 Overview
`RemoteImport.py` automates the transition of data from the acquisition workstation to the OMERO server. It supports two primary import modes based on the selected target:

1.  **Target = Project**: The script scans the user's import folder. For every sub-directory found, it automatically creates a new **Dataset** within the project, mirroring the folder structure (e.g., `/dirA/dirB` becomes a dataset named `dirA_dirB`).
2.  **Target = Dataset**: All images found within the source folder (including those in sub-directories) are imported into a single, specified Dataset.

## 🛠 Key Features
*   **In-Place Import**: When `INPLACE_IMPORT = True`, the script utilizes symbolic links (`ln_s`) to avoid duplicating massive image files on the server.
*   **Data Staging**: If `COPY_SOURCES = True`, the script first transfers data to a secure server path (`DATA_PATH`) organized by user ID and timestamp before initiating the import.
*   **Hybrid Import**: In addition to images, the script can identify and attach non-image files (e.g., `.txt`, `.pdf`, `.csv`) as annotations to the imported objects.
*   **Robustness**: Includes a retry mechanism for failed imports and generates a log report attached to the dataset.

## ⚙️ Administrator Configuration
The following constants in the script must be configured to match the server environment:
*   `MOUNT_PATH`: Root directory where workstations are mounted (e.g., `/Importer/`).
*   `WORKSTATION_NAMES`: List of supported workstations (e.g., `["ws01", "ws02"]`).
*   `DATA_PATH`: The storage path for staged data (`/storage/OMERO_inplace/users/`).
*   `INPLACE_IMPORT` / `COPY_SOURCES`: Boolean flags to toggle the transfer and linking behavior.

## 🖥 User Guide
The script is executed via the OMERO.web interface.

### Parameters:
*   **Workstations**: Select the source workstation.
*   **Data Type**: Choose `Project` (to create multiple datasets) or `Dataset` (to merge all into one).
*   **IDs**: The ID of the target OMERO object.
*   **Attach non-image files**: If checked, the script will look for metadata files.
*   **Attach to object type**: Specify if attachments should be linked to the Project or Dataset.
*   **Filter attachment**: A comma-separated list of extensions to include (e.g., `txt, csv`).

## 📋 Requirements
*   **OMERO SERVER**: The server must have the `omero-cli` installed and accessible to the Python environment.
*   **NFS/Samba Mounts**: Workstations must be automounted under the configured `MOUNT_PATH`.
*   **Permissions**: The OMERO service user requires write permissions to `DATA_PATH` and the OMERO data directory.

## ⚖️ License
This software is released under the **GNU General Public License v3 (GPL-3.0-or-later)**.

***

# OMERO FetchAndAttach

A server-side OMERO script that automates the retrieval of microscopy data from networked workstations and attaches them as in-place attachments to OMERO objects (e.g., Projects or Datasets).

## 🚀 Overview
Instead of manually uploading files, the script:
1. Scans a specific user folder on a designated workstation.
2. Copies the identified files to a managed local storage path on the OMERO server.
3. Creates a symbolic link in the OMERO data directory (in-place attachment).
4. Links the file annotation to the selected OMERO object.

## 🛠 Configuration (Admin)
Before deploying, update the `########## CONFIG ##########` section in the script:
*   `DATA_PATH`: The root directory on the server where files will be stored (`/storage/OMERO_inplace/users/`).
*   `MOUNT_PATH`: The base path where workstations are automounted (`/Importer/`).
*   `WORKSTATION_NAMES`: A list of valid workstation identifiers (e.g., `["ws01", "ws02"]`).
*   `OMERO_DATA_DIR`: The path to the OMERO managed data root.

## 🖥 User Guide
Once installed, the script is available in the OMERO.web "Scripts" menu.

### Parameters:
1. **Workstations**: Select the computer where your data is currently located. (**Note:** The computer must be powered on and connected to the network).
2. **Data Type**: Choose whether you are attaching the data to a `Project` or a `Dataset`.
3. **IDs**: Enter the unique ID of the destination object.
4. **Filter**: (Optional) Enter file extensions to filter for (e.g., `txt, pdf, zip`). If left blank, all files in the directory will be fetched.

## 📂 Data Structure
The script organizes fetched data on the server using a timestamped hierarchy to prevent overwriting:
`DATA_PATH / <username>_<userID> / YYYY-MM / DD / HH-mm-ss.SSS /`

## 📋 Requirements
*   **OMERO Server** with `omero-upload` utility installed.
*   **NFS/Samba Mounts**: Workstations must be automounted under the configured `MOUNT_PATH`.
*   **Permissions**: The user running the OMERO server process must have read access to the mounts and write access to the `DATA_PATH`.

## ⚖️ License
This project is licensed under the **GPL v3**.
