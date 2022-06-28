from dcm.tasks.services.uploads import UploadConnectorHandler, UploadCollectionConnectorHandler
from dcm.tasks.services.imports import ImportConnectorHandler, ImportManualHandler, ImportApiHandler
from dcm.tasks.services.transforms import ConcatHandler, JoinHandler, TransformationHandler, PipelineHandler, PycodeHandler
from dcm.tasks.services.check import CheckHandler


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
    elif task_type == "pycode":
        handler = PycodeHandler
    elif task_type in CheckHandler.check_type:
        handler = CheckHandler
    elif task_type == "import_api":
        handler = ImportApiHandler
    else:
        raise Exception(f'NO HANDLER FOUND FOR {task_type} TYPE')

    handler(context).start()

    return