from dcm.tasks.dcm.uploads import UploadConnectorHandler, UploadCollectionConnectorHandler
from dcm.tasks.dcm.imports import ImportConnectorHandler, ImportManualHandler
from dcm.tasks.dcm.transforms import ConcatHandler, JoinHandler, TransformationHandler, PipelineHandler


def dcm_hook_factory(**context):
    
    task_type = context['task_type']
    
    handler = None
    if task_type in ImportConnectorHandler.import_connector_types:
        handler = ImportConnectorHandler
    elif task_type in TransformationHandler.transformation_types:
        handler = TransformationHandler
    elif task_type == 'concat':
        handler = ConcatHandler
    elif task_type == 'join':
        handler = JoinHandler
    elif task_type == 'PIPELINE_TRANSFORMATION':
        handler = PipelineHandler
    elif task_type in UploadConnectorHandler.upload_connector_types:
        handler = UploadConnectorHandler
    elif task_type == "COLLECTION_UPLOAD":
        handler = UploadCollectionConnectorHandler
    elif task_type == "IMPORT_MANUAL":
        handler = ImportManualHandler
    else:
        raise Exception(f'NO HANDLER FOUND FOR {task_type} TYPE')

    handler(context).start()

    return