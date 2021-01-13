from dcm_plugin.tasks.dcm.uploads import UploadConnectorHandler
from dcm_plugin.tasks.dcm.imports import ImportConnectorHandler
from dcm_plugin.tasks.dcm.transforms import ConcatHandler, JoinHandler, TransformationHandler


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
    elif task_type in UploadConnectorHandler.upload_connector_types:
        handler = UploadConnectorHandler
    else:
        raise f'NO HANDLER FOUND FOR {task_type} TYPE'

    handler(context).start()

    return