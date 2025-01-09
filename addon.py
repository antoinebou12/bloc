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

cached_file_list = []
cached_tree = {}
error_messages = []
expanded_folders = set()
is_refreshing = False

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

class CloudConnection(bpy.types.PropertyGroup):
    name: bpy.props.StringProperty(name="Connection Name", default="New Connection")
    cloud_type: bpy.props.EnumProperty(
        name="Cloud Type",
        items=[("minio", "MinIO", ""), ("s3", "AWS S3", "")]
    )
    endpoint_url: bpy.props.StringProperty(name="Endpoint URL", default="")
    access_key: bpy.props.StringProperty(name="Access Key", default="", subtype='PASSWORD')
    secret_key: bpy.props.StringProperty(name="Secret Key", default="", subtype='PASSWORD')
    region_name: bpy.props.StringProperty(name="Region Name", default="us-east-1")
    bucket_name: bpy.props.StringProperty(name="Bucket Name", default="")

class CLOUD_OT_ConnectionAdd(bpy.types.Operator):
    bl_idname = "cloud.connection_add"
    bl_label = "Add Cloud Connection"

    def execute(self, context):
        prefs = context.preferences.addons[__name__].preferences
        new_conn = prefs.connections.add()
        prefs.active_connection_index = len(prefs.connections) - 1
        return {'FINISHED'}

class CLOUD_OT_ConnectionRemove(bpy.types.Operator):
    bl_idname = "cloud.connection_remove"
    bl_label = "Remove Cloud Connection"

    def execute(self, context):
        prefs = context.preferences.addons[__name__].preferences
        if prefs.connections:
            prefs.connections.remove(prefs.active_connection_index)
            prefs.active_connection_index = max(0, prefs.active_connection_index - 1)
        return {'FINISHED'}

def get_active_connection():
    prefs = bpy.context.preferences.addons[__name__].preferences
    if prefs.connections:
        return prefs.connections[prefs.active_connection_index]
    return None

def initialize_cloud_client():
    global s3_client, minio_client
    conn = get_active_connection()
    if not conn:
        logger.error("No active cloud connection.")
        return

    if conn.cloud_type == "minio":
        from minio import Minio
        minio_client = Minio(
            conn.endpoint_url.replace("http://", "").replace("https://", ""),
            access_key=conn.access_key,
            secret_key=conn.secret_key,
            secure=conn.endpoint_url.startswith("https://")
        )
    elif conn.cloud_type == "s3":
        import boto3
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=conn.access_key,
            aws_secret_access_key=conn.secret_key,
            region_name=conn.region_name
        )

class CloudIntegrationPreferences(bpy.types.AddonPreferences):
    bl_idname = __name__

    connections: bpy.props.CollectionProperty(type=CloudConnection)
    active_connection_index: bpy.props.IntProperty(default=0)

    def draw(self, context):
        layout = self.layout
        row = layout.row()
        row.template_list("UI_UL_list", "cloud_connections", self, "connections", self, "active_connection_index")

        col = row.column(align=True)
        col.operator("cloud.connection_add", icon="ADD", text="")
        col.operator("cloud.connection_remove", icon="REMOVE", text="")

        if self.connections:
            conn = self.connections[self.active_connection_index]
            layout.prop(conn, "name")
            layout.prop(conn, "cloud_type")
            if conn.cloud_type == "minio":
                layout.prop(conn, "endpoint_url")
            elif conn.cloud_type == "s3":
                layout.prop(conn, "region_name")
            layout.prop(conn, "access_key")
            layout.prop(conn, "secret_key")
            layout.prop(conn, "bucket_name")

class CloudIntegrationPanel(bpy.types.Panel):
    bl_label = "Cloud Integration"
    bl_idname = "VIEW3D_PT_cloud_integration"
    bl_space_type = 'VIEW_3D'
    bl_region_type = 'UI'
    bl_category = "Cloud"

    def draw(self, context):
        layout = self.layout

        row = layout.row(align=True)
        row.operator("cloud.upload_file", text="Upload Current File")
        row.operator("cloud.update_file_list", text="Refresh File List")

        if is_refreshing:
            layout.label(text="Refreshing...", icon="FILE_REFRESH")
            return
            
        if error_messages:
            box = layout.box()
            box.label(text="Errors:", icon="ERROR")
            for msg in error_messages[-5:]:
                box.label(text=msg, icon="CANCEL")
            error_messages.clear()

        if not cached_tree:
            layout.label(text="No files in the bucket.", icon="INFO")
        else:
            draw_file_tree(layout, cached_tree)

def build_file_tree(file_list):
    tree = {}
    for item in file_list:
        parts = item.name.split("/")
        current_level = tree
        for part in parts:
            if part not in current_level:
                current_level[part] = {}
            current_level = current_level[part]
    return tree

def draw_file_tree(layout, tree, path=""):
    for key, value in tree.items():
        current_path = os.path.join(path, key) if path else key
        if value:
            row = layout.row(align=True)
            icon = "TRIA_DOWN" if current_path in expanded_folders else "TRIA_RIGHT"
            op = row.operator("cloud.toggle_folder", text="", icon=icon, emboss=False)
            op.folder_path = current_path
            row.label(text=f"{key}/", icon="FILE_FOLDER")
            if current_path in expanded_folders:
                sub_box = layout.box()
                draw_file_tree(sub_box, value, current_path)
        else:
            row = layout.row()
            lower_key = key.lower()
            icon_type = ("MESH_DATA" if lower_key.endswith((".stl", ".obj", ".ply", ".usd", ".usda", ".usdc"))
                         else ("ERROR" if lower_key.endswith(".gbx") else "FILE"))
            row.label(text=key, icon=icon_type)
            ops_row = layout.row(align=True)
            download_op = ops_row.operator("cloud.download_file", text="Download")
            download_op.file_name = current_path
            delete_op = ops_row.operator("cloud.delete_file", text="Delete")
            delete_op.file_name = current_path
            if lower_key.endswith((".stl", ".obj", ".ply", ".usd", ".usda", ".usdc")):
                load_op = ops_row.operator("cloud.load_file", text="Load to Scene")
                load_op.file_name = current_path

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

def schedule_update_scene_collection(files):
    def update_scene_collection():
        context = bpy.context
        context.scene.cloud_file_list.clear()
        for file in files:
            new_item = context.scene.cloud_file_list.add()
            new_item.name = file
        global is_refreshing
        is_refreshing = False
        for area in bpy.context.screen.areas:
            if area.type == 'VIEW_3D':
                area.tag_redraw()
        return None
    bpy.app.timers.register(update_scene_collection, first_interval=0.1)

class CloudUpdateFileListOperator(bpy.types.Operator):
    bl_idname = "cloud.update_file_list"
    bl_label = "Update File List"

    def execute(self, context):
        global is_refreshing
        is_refreshing = True

        def background_update():
            global cached_file_list, cached_tree
            files = list_files_in_bucket()
            cached_file_list = files

            tree = {}
            for file in files:
                parts = file.split("/")
                current_level = tree
                for part in parts:
                    if part not in current_level:
                        current_level[part] = {}
                    current_level = current_level[part]
            cached_tree = tree

            schedule_update_scene_collection(files)

        threading.Thread(target=background_update, daemon=True).start()
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

class CloudLoadFileOperator(bpy.types.Operator):
    bl_idname = "cloud.load_file"
    bl_label = "Load File to Scene"

    file_name: bpy.props.StringProperty()

    def execute(self, context):
        temp_dir = tempfile.gettempdir()
        initialize_cloud_client()

        try:
            local_file_path = download_file(self.file_name, temp_dir)
            if not local_file_path or not os.path.exists(local_file_path):
                self.report({"ERROR"}, f"File {self.file_name} does not exist or failed to download.")
                return {"CANCELLED"}

            filepath_lower = local_file_path.lower()
            if filepath_lower.endswith(".stl"):
                bpy.ops.wm.stl_import(filepath=local_file_path)
                message = f"Loaded {self.file_name} (STL) into the Blender scene."
            elif filepath_lower.endswith(".obj"):
                bpy.ops.wm.obj_import(filepath=local_file_path)
                message = f"Loaded {self.file_name} (OBJ) into the Blender scene."
            elif filepath_lower.endswith(".usd") or filepath_lower.endswith(".usda") or filepath_lower.endswith(".usdc"):
                bpy.ops.wm.usd_import(filepath=local_file_path)
                message = f"Loaded {self.file_name} (USD) into the Blender scene."
            elif filepath_lower.endswith(".ply"):
                bpy.ops.wm.ply_import(filepath=local_file_path)
                message = f"Loaded {self.file_name} (PLY) into the Blender scene."
            elif filepath_lower.endswith(".gbx"):
                self.report({"ERROR"}, "GBX file format is not supported.")
                return {"CANCELLED"}
            else:
                self.report({"ERROR"}, f"{self.file_name} is not a supported file format.")
                return {"CANCELLED"}

            logger.info(message)
            self.report({"INFO"}, message)

        except Exception as e:
            error_messages.append(str(e))
            logger.error(f"Error loading file: {e}")
            self.report({"ERROR"}, f"Failed to load file: {e}")
            return {"CANCELLED"}

        return {"FINISHED"}

class CloudDownloadFileOperator(bpy.types.Operator):
    bl_idname = "cloud.download_file"
    bl_label = "Download File from Bucket"

    file_name: bpy.props.StringProperty()

    def execute(self, context):
        temp_dir = tempfile.gettempdir()
        initialize_cloud_client()

        try:
            local_file_path = download_file(self.file_name, temp_dir)
            if local_file_path:
                self.report({"INFO"}, f"Downloaded {self.file_name}")
            else:
                self.report({"ERROR"}, f"Failed to download file: {self.file_name}")
                return {"CANCELLED"}
        except Exception as e:
            error_messages.append(str(e))
            logger.error(f"Error downloading file: {e}")
            self.report({"ERROR"}, f"Failed to download file: {e}")
            return {"CANCELLED"}

        return {"FINISHED"}
        
class CloudDeleteFileOperator(bpy.types.Operator):
    bl_idname = "cloud.delete_file"
    bl_label = "Delete File from Bucket"

    file_name: bpy.props.StringProperty()

    def execute(self, context):
        try:
            conn = get_active_connection()
            if not conn:
                self.report({"ERROR"}, "No active cloud connection.")
                return {"CANCELLED"}

            if conn.cloud_type == "minio":
                minio_client.remove_object(conn.bucket_name, self.file_name)
            else:
                s3_client.delete_object(Bucket=conn.bucket_name, Key=self.file_name)

            bpy.ops.cloud.update_file_list()
            self.report({"INFO"}, f"Deleted {self.file_name}")
        except Exception as e:
            error_messages.append(str(e))
            logger.error(f"Error deleting file: {e}")
            self.report({"ERROR"}, f"Failed to delete file: {e}")
            return {"CANCELLED"}

        return {"FINISHED"}
        
def list_files_in_bucket():
    conn = get_active_connection()
    if not conn:
        logger.error("No active cloud connection.")
        return []

    # Initialize the client if not already done
    if conn.cloud_type == "minio" and not minio_client:
        initialize_cloud_client()
    elif conn.cloud_type == "s3" and not s3_client:
        initialize_cloud_client()

    try:
        if conn.cloud_type == "minio":
            objects = minio_client.list_objects(conn.bucket_name, recursive=True)
            return [obj.object_name for obj in objects]
        else:
            response = s3_client.list_objects_v2(Bucket=conn.bucket_name)
            return [obj['Key'] for obj in response.get('Contents', [])]
    except Exception as e:
        error_messages.append(str(e))
        logger.error(f"Error listing files: {e}")
        return []

def upload_file(local_path, remote_name):
    conn = get_active_connection()
    if not conn:
        logger.error("No active cloud connection.")
        return False

    try:
        if conn.cloud_type == "minio":
            minio_client.fput_object(conn.bucket_name, remote_name, local_path)
        else:
            s3_client.upload_file(local_path, conn.bucket_name, remote_name)
        return True
    except Exception as e:
        error_messages.append(str(e))
        logger.error(f"Error uploading file: {e}")
        return False

def download_file(remote_name, local_dir):
    conn = get_active_connection()
    if not conn:
        logger.error("No active cloud connection.")
        return None

    try:
        local_path = os.path.join(local_dir, os.path.basename(remote_name))
        if conn.cloud_type == "minio":
            minio_client.fget_object(conn.bucket_name, remote_name, local_path)
        else:
            s3_client.download_file(conn.bucket_name, remote_name, local_path)
        return local_path
    except Exception as e:
        error_messages.append(str(e))
        logger.error(f"Error downloading file: {e}")
        return None

def register():
    bpy.utils.register_class(CloudConnection)
    bpy.utils.register_class(CLOUD_OT_ConnectionAdd)
    bpy.utils.register_class(CLOUD_OT_ConnectionRemove)
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
    # Initialize default preferences if not set
    addon = bpy.context.preferences.addons.get(__name__)
    if addon:
        prefs = addon.preferences
        if not prefs.connections:
            # Create a default connection
            conn = prefs.connections.add()
            conn.name = "Default Connection"
            conn.cloud_type = "s3"
            conn.endpoint_url = ""
            conn.access_key = ""
            conn.secret_key = ""
            conn.region_name = "us-east-1"
            conn.bucket_name = ""

def unregister():
    bpy.utils.unregister_class(CloudConnection)
    bpy.utils.unregister_class(CLOUD_OT_ConnectionAdd)
    bpy.utils.unregister_class(CLOUD_OT_ConnectionRemove)
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

