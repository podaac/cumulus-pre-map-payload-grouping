"""
An AWS Lambda which works with the Cumulus framework to allow users to map
CMR payloads into groups for other cumulus modules
"""

import os
from cumulus_logger import CumulusLogger
from cumulus_process import Process

logger = CumulusLogger('cumulus_pre_map_payload_grouping')

class SplitAndGroupPayloadGranules(Process):
    """
    The main class which `process` is overloaded via cumulus_process;
    """
    className = "SplitAndGroupPayloadGranules"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logger
        self.logger.debug('{} Entered __init__', self.className)

    def process(self):
        cumulus_meta = self.config.get('cumulus_meta')
        meta = self.config.get('meta')

        granules_per_group = os.environ.get('granules_per_group', 3)

        if 'granules' not in self.input.keys():
            raise KeyError('"granules" is missing from self.input')

        self.logger.debug('collection: {} | granules found: {}',
                          meta.get('collection').get('name'),
                          len(self.input['granules']))

        grouped_inputs = []
        for i in range(0, len(self.input.get('granules')), int(granules_per_group)):
            grouped_inputs.append(self.input.get('granules')[i:i + int(granules_per_group)])

        cma_groups = []
        for grouped_input in grouped_inputs:
            cma_groups.append({
                "cma": {
                    "task_config": {
                        "provider": "{$.meta.provider}",
                        "provider_path": "{$.meta.collection.meta.provider_path}",
                        "collection": "{$.meta.collection}",
                        "cumulus_meta": "{$.cumulus_meta}",
                        "cumulus_message": ""
                    },
                    "event": {
                        "cumulus_meta": cumulus_meta,
                        "meta": meta,
                        "payload": {
                            "granules": grouped_input
                        },
                        "exception": None
                    }
                }
            })

        return cma_groups


def lambda_handler(event, context):
    """
    AWS Lambdas signature function called by AWS Step Functions
    """
    logger.setMetadata(event, context)
    return SplitAndGroupPayloadGranules.cumulus_handler(event, context=context)
