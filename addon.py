bl_info = {
    "name": "Blender MinIO/S3 Integration",
    "author": "Kashish aka MrKuros",
    "version": (3, 0),
    "blender": (4, 3, 0),
    "location": "View3D > Tool Shelf > Cloud Integration",
    "description": "Upload, download, and manage Blender files in MinIO or AWS S3",
    "category": "Development",
}

import bpy
import os
import sys
import logging
import subprocess
import threading
import tempfile
from pathlib import Path

# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

REQUIRED_PACKAGES = ["boto3", "minio"]

# Ensure Python modules are accessible
def get_modules_path():
    return bpy.utils.user_resource("SCRIPTS", path="modules", create=True)

def append_modules_to_sys_path(modules_path):
    if modules_path not in sys.path:
        sys.path.append(modules_path)

def background_install_packages(packages, modules_path):
    """Install the required Python packages in the background."""
    def install_packages():
        bpy.context.window_manager.progress_begin(0, len(packages))
        for i, package in enumerate(packages):
            try:
                __import__(package)
                logger.info(f"'{package}' is already installed.")
            except ImportError:
                logger.info(f"Installing '{package}'...")
                try:
                    subprocess.check_call([
                        sys.executable,
                        "-m",
                        "pip",
                        "install",
                        "--upgrade",
                        "--target",
                        modules_path,
                        package
                    ])
                    logger.info(f"'{package}' installed successfully.")
                except subprocess.CalledProcessError as e:
                    logger.error(f"Failed to install '{package}'. Error: {e}")
        bpy.context.window_manager.progress_end()

    threading.Thread(target=install_packages, daemon=True).start()

modules_path = get_modules_path()
append_modules_to_sys_path(modules_path)
background_install_packages(REQUIRED_PACKAGES, modules_path)

# Global clients
s3_client = None
minio_client = None

class CloudIntegrationPreferences(bpy.types.AddonPreferences):
    bl_idname = __name__

    cloud_type: bpy.props.EnumProperty(
        name="Cloud Type",
        description="Choose the cloud provider",
        items=[("minio", "MinIO", ""), ("s3", "AWS S3", "")],
        default="minio"
    )
    endpoint_url: bpy.props.StringProperty(
        name="Endpoint URL",
        description="MinIO Endpoint URL (e.g., http://127.0.0.1:9000)",
        default=""
    )
    access_key: bpy.props.StringProperty(
        name="Access Key",
        description="Access Key",
        default="",
        subtype='PASSWORD'
    )
    secret_key: bpy.props.StringProperty(
        name="Secret Key",
        description="Secret Key",
        default="",
        subtype='PASSWORD'
    )
    region_name: bpy.props.StringProperty(
        name="Region Name",
        description="AWS Region Name (S3 only)",
        default="us-east-1"
    )
    bucket_name: bpy.props.StringProperty(
        name="Bucket Name",
        description="Bucket Name",
        default=""
    )

    def draw(self, context):
        layout = self.layout
        layout.prop(self, "cloud_type")
        if self.cloud_type == "minio":
            layout.prop(self, "endpoint_url")
        elif self.cloud_type == "s3":
            layout.prop(self, "region_name")
        layout.prop(self, "access_key")
        layout.prop(self, "secret_key")
        layout.prop(self, "bucket_name")

def initialize_cloud_client():
    """Initialize the appropriate cloud client based on user preferences."""
    global s3_client, minio_client
    prefs = bpy.context.preferences.addons[__name__].preferences

    if prefs.cloud_type == "minio":
        from minio import Minio
        minio_client = Minio(
            prefs.endpoint_url.replace("http://", "").replace("https://", ""),
            access_key=prefs.access_key,
            secret_key=prefs.secret_key,
            secure=prefs.endpoint_url.startswith("https://")
        )
    elif prefs.cloud_type == "s3":
        import boto3
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=prefs.access_key,
            aws_secret_access_key=prefs.secret_key,
            region_name=prefs.region_name
        )

def list_files_in_bucket():
    """List files in the configured bucket."""
    prefs = bpy.context.preferences.addons[__name__].preferences
    initialize_cloud_client()
    try:
        if prefs.cloud_type == "minio":
            objects = minio_client.list_objects(prefs.bucket_name, recursive=True)
            return [obj.object_name for obj in objects]
        elif prefs.cloud_type == "s3":
            response = s3_client.list_objects_v2(Bucket=prefs.bucket_name)
            return [content["Key"] for content in response.get("Contents", [])]
    except Exception as e:
        logger.error(f"Error listing files in bucket: {e}")
        return []

def upload_file(local_file_path, s3_key):
    prefs = bpy.context.preferences.addons[__name__].preferences
    initialize_cloud_client()
    try:
        # Normalize object key to use forward slashes
        normalized_key = s3_key.replace("\\", "/")
        logger.info(f"Attempting to upload file: {local_file_path} as {normalized_key} to bucket: {prefs.bucket_name}")
        
        if prefs.cloud_type == "minio":
            minio_client.fput_object(prefs.bucket_name, normalized_key, local_file_path)
        elif prefs.cloud_type == "s3":
            s3_client.upload_file(local_file_path, prefs.bucket_name, normalized_key)
        
        logger.info(f"Uploaded {local_file_path} as {normalized_key} to bucket {prefs.bucket_name}.")
    except Exception as e:
        logger.error(f"Error uploading file: {e}")


def download_file(s3_key, local_dir):
    prefs = bpy.context.preferences.addons[__name__].preferences
    initialize_cloud_client()
    try:
        # Normalize object key to use forward slashes
        normalized_key = s3_key.replace("\\", "/")
        logger.info(f"Attempting to download file: {normalized_key} from bucket: {prefs.bucket_name}")
        
        local_file_path = os.path.join(local_dir, os.path.basename(normalized_key))
        if prefs.cloud_type == "minio":
            minio_client.fget_object(prefs.bucket_name, normalized_key, local_file_path)
        elif prefs.cloud_type == "s3":
            s3_client.download_file(prefs.bucket_name, normalized_key, local_file_path)
        
        logger.info(f"Downloaded {normalized_key} to {local_file_path}.")
        return local_file_path
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        return None


        
class CloudDeleteFileOperator(bpy.types.Operator):
    bl_idname = "cloud.delete_file"
    bl_label = "Delete File from Bucket"

    file_name: bpy.props.StringProperty()

    def execute(self, context):
        prefs = bpy.context.preferences.addons[__name__].preferences
        try:
            if prefs.cloud_type == "minio":
                minio_client.remove_object(prefs.bucket_name, self.file_name)
            elif prefs.cloud_type == "s3":
                s3_client.delete_object(Bucket=prefs.bucket_name, Key=self.file_name)
            logger.info(f"Deleted file: {self.file_name}")
        except Exception as e:
            logger.error(f"Error deleting file: {e}")
            self.report({"ERROR"}, f"Failed to delete file: {e}")
            return {"CANCELLED"}

        # Refresh the file list after deletion
        bpy.ops.cloud.update_file_list()
        return {"FINISHED"}


class CloudIntegrationPanel(bpy.types.Panel):
    bl_label = "Cloud Integration"
    bl_idname = "VIEW3D_PT_cloud_integration"
    bl_space_type = 'VIEW_3D'
    bl_region_type = 'UI'
    bl_category = "Cloud"

    def draw(self, context):
        layout = self.layout

        # Cloud Preferences
        prefs = bpy.context.preferences.addons[__name__].preferences

        # Buttons for operations
        layout.operator("cloud.upload_file", text="Upload Current File")
        layout.operator("cloud.update_file_list", text="Refresh File List")

        # Display hierarchical file tree
        files = build_file_tree(context.scene.cloud_file_list)

        if not files:
            layout.label(text="No files in the bucket.", icon="INFO")
        else:
            draw_file_tree(layout, files)

def build_file_tree(file_list):
    """Build a hierarchical tree from a flat list of file paths."""
    tree = {}
    for item in file_list:
        parts = item.name.split("/")
        current_level = tree
        for part in parts:
            if part not in current_level:
                current_level[part] = {}
            current_level = current_level[part]
    return tree
    
def draw_file_tree_with_bucket(layout, tree, bucket_name, path=""):
    """Recursively draw the file tree in the Blender UI with bucket name."""
    layout.label(text=f"Bucket: {bucket_name}", icon="FILE_VOLUME")
    draw_file_tree(layout, tree, path)
    
def draw_file_tree(layout, tree, path=""):
    """Recursively draw the file tree in the Blender UI with collapsible folders."""
    for key, value in tree.items():
        current_path = os.path.join(path, key) if path else key
        if value:  # It's a folder
            row = layout.row(align=True)
            # Determine icon based on expansion state
            icon = "TRIA_DOWN" if current_path in expanded_folders else "TRIA_RIGHT"
            # Create a button that toggles folder state
            op = row.operator("cloud.toggle_folder", text="", icon=icon, emboss=False)
            op.folder_path = current_path
            
            # Folder label
            row.label(text=f"{key}/", icon="FILE_FOLDER")
            
            # If expanded, recurse into folder contents
            if current_path in expanded_folders:
                # Indent children items for visual hierarchy
                sub_box = layout.box()
                draw_file_tree(sub_box, value, current_path)
        else:  # It's a file
            row = layout.row()
            # Display file icon based on type
            icon_type = "FILE_BLEND" if key.endswith(".blend") else "MESH_DATA"
            row.label(text=key, icon=icon_type)
            
            # Operations for the file
            ops_row = layout.row(align=True)
            
            # Add Download button
            download_op = ops_row.operator("cloud.download_file", text="Download")
            download_op.file_name = current_path  # Use full path for nested files
            
            # Add Delete button
            delete_op = ops_row.operator("cloud.delete_file", text="Delete")
            delete_op.file_name = current_path  # Use full path for nested files
            
            # Add Load button for STL files
            if key.endswith(".stl"):
                load_op = ops_row.operator("cloud.load_file", text="Load to Scene")
                load_op.file_name = current_path  # Use full path for STL files

expanded_folders = set()

class CloudToggleFolderOperator(bpy.types.Operator):
    bl_idname = "cloud.toggle_folder"
    bl_label = "Toggle Folder"

    folder_path: bpy.props.StringProperty()

    def execute(self, context):
        global expanded_folders
        if self.folder_path in expanded_folders:
            expanded_folders.remove(self.folder_path)
        else:
            expanded_folders.add(self.folder_path)
        return {'FINISHED'}

class CloudFilePropertyGroup(bpy.types.PropertyGroup):
    name: bpy.props.StringProperty()

class CloudUpdateFileListOperator(bpy.types.Operator):
    bl_idname = "cloud.update_file_list"
    bl_label = "Update File List"

    def execute(self, context):
        context.scene.cloud_file_list.clear()
        for file in list_files_in_bucket():
            new_item = context.scene.cloud_file_list.add()
            new_item.name = file
        return {"FINISHED"}

class CloudUploadFileOperator(bpy.types.Operator):
    bl_idname = "cloud.upload_file"
    bl_label = "Upload Current Blender File"

    def execute(self, context):
        local_file_path = bpy.data.filepath
        if not local_file_path:
            self.report({"ERROR"}, "Please save the Blender file first.")
            return {"CANCELLED"}
        upload_file(local_file_path, os.path.basename(local_file_path))
        bpy.ops.cloud.update_file_list()
        return {"FINISHED"}

class CloudDownloadFileOperator(bpy.types.Operator):
    bl_idname = "cloud.download_file"
    bl_label = "Download File from Bucket"

    file_name: bpy.props.StringProperty()

    def execute(self, context):
        prefs = bpy.context.preferences.addons[__name__].preferences
        temp_dir = tempfile.gettempdir()  # Temporary directory for downloads
        initialize_cloud_client()

        try:
            # Download the file
            local_file_path = download_file(self.file_name, temp_dir)
            if local_file_path:
                logger.info(f"Downloaded {self.file_name} to {local_file_path}")
                self.report({"INFO"}, f"Downloaded {self.file_name}")
            else:
                logger.error(f"Failed to download file: {self.file_name}")
                self.report({"ERROR"}, f"Failed to download file: {self.file_name}")
                return {"CANCELLED"}

        except Exception as e:
            logger.error(f"Error downloading file: {e}")
            self.report({"ERROR"}, f"Failed to download file: {e}")
            return {"CANCELLED"}

        return {"FINISHED"}
        
class CloudLoadFileOperator(bpy.types.Operator):
    bl_idname = "cloud.load_file"
    bl_label = "Load STL File to Scene"

    file_name: bpy.props.StringProperty()

    def execute(self, context):
        prefs = bpy.context.preferences.addons[__name__].preferences
        temp_dir = tempfile.gettempdir()  # Temporary directory for downloads
        initialize_cloud_client()

        try:
            # Download the file
            local_file_path = download_file(self.file_name, temp_dir)

            if local_file_path and os.path.exists(local_file_path):
                if local_file_path.endswith(".stl"):
                    bpy.ops.wm.stl_import(filepath=local_file_path)
                    logger.info(f"Loaded {self.file_name} into the Blender scene.")
                    self.report({"INFO"}, f"Loaded {self.file_name} into the Blender scene.")
                else:
                    self.report({"ERROR"}, f"{self.file_name} is not a valid .stl file.")
            else:
                self.report({"ERROR"}, f"File {self.file_name} does not exist in the bucket.")
                logger.error(f"File {self.file_name} does not exist or failed to download.")
                return {"CANCELLED"}

        except Exception as e:
            logger.error(f"Error loading file: {e}")
            self.report({"ERROR"}, f"Failed to load file: {e}")
            return {"CANCELLED"}

        return {"FINISHED"}

def register():
    bpy.utils.register_class(CloudIntegrationPreferences)
    bpy.utils.register_class(CloudIntegrationPanel)
    bpy.utils.register_class(CloudUpdateFileListOperator)
    bpy.utils.register_class(CloudUploadFileOperator)
    bpy.utils.register_class(CloudDeleteFileOperator)
    bpy.utils.register_class(CloudLoadFileOperator)
    bpy.utils.register_class(CloudDownloadFileOperator)
    bpy.utils.register_class(CloudToggleFolderOperator)
    bpy.utils.register_class(CloudFilePropertyGroup)
    bpy.types.Scene.cloud_file_list = bpy.props.CollectionProperty(type=CloudFilePropertyGroup)

def unregister():
    bpy.utils.unregister_class(CloudIntegrationPreferences)
    bpy.utils.unregister_class(CloudIntegrationPanel)
    bpy.utils.unregister_class(CloudUpdateFileListOperator)
    bpy.utils.unregister_class(CloudUploadFileOperator)
    bpy.utils.unregister_class(CloudDeleteFileOperator)
    bpy.utils.unregister_class(CloudLoadFileOperator)
    bpy.utils.unregister_class(CloudDownloadFileOperator)
    bpy.utils.unregister_class(CloudToggleFolderOperator)
    bpy.utils.unregister_class(CloudFilePropertyGroup)
    del bpy.types.Scene.cloud_file_list

if __name__ == "__main__":
    register()
