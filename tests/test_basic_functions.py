import os
import boto3
import json

from cumulus_pre_map_payload_grouping.cumulus_pre_map_payload_grouping import lambda_handler
from moto import mock_s3


lambda_input = {
    "cma": {
        "event": {
            "cumulus_meta": {
                "cumulus_meta_dummy_key": "dummy_value"
            },
            "exception": "None",
            "meta": {
                "collection": {
                    "name": "dummy_collection_name"
                }
            },
            "payload": {
                "granules": [
                    {
                        "granuleId": "S6A_AX____POE__AX_20230524T215923_20230526T015923_20230614T150335__________________SALP_OPE_NT_002.SEN6.tar",
                        "dataType": "MASTER_S6_CLOUD_COLLECTIONS",
                        "version": "0.1",
                        "files": [
                            {
                                "name": "S6A_AX____POE__AX_20230524T215923_20230526T015923_20230614T150335__________________SALP_OPE_NT_002.SEN6.tar",
                                "path": "S6A",
                                "size": 1024000,
                                "time": 1717620499438,
                                "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                                "bucket": "hryeung-ia-podaac-protected",
                                "type": "data"
                            }
                        ]
                    },
                    {
                        "granuleId": "S6A_DO_0__DOP_____20201203T103532_20201203T123115_20201203T123145_6943_002_047_024_EUM__OPE_NR____.SEN6.tar",
                        "dataType": "MASTER_S6_CLOUD_COLLECTIONS",
                        "version": "0.1",
                        "files": [
                            {
                                "name": "S6A_DO_0__DOP_____20201203T103532_20201203T123115_20201203T123145_6943_002_047_024_EUM__OPE_NR____.SEN6.tar",
                                "path": "S6A",
                                "size": 1617920,
                                "time": 1717620499438,
                                "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                                "bucket": "hryeung-ia-podaac-protected",
                                "type": "data"
                            }
                        ]
                    },
                    {
                        "granuleId": "S6A_GN_1B_RNXH_AX_20230614T125942_20230614T135941_20230614T153905__________________CPOD_OPE_NR____.SEN6.tar",
                        "dataType": "MASTER_S6_CLOUD_COLLECTIONS",
                        "version": "0.1",
                        "files": [
                            {
                                "name": "S6A_GN_1B_RNXH_AX_20230614T125942_20230614T135941_20230614T153905__________________CPOD_OPE_NR____.SEN6.tar",
                                "path": "S6A",
                                "size": 1034240,
                                "time": 1717620499438,
                                "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                                "bucket": "hryeung-ia-podaac-protected",
                                "type": "data"
                            }
                        ]
                    },
                    {
                        "granuleId": "S6A_MW_0__AMR_____20201203T050820_20201203T050936_20201203T104056_0076_002_041_021_EUM__OPE_ND____.SEN6.tar",
                        "dataType": "MASTER_S6_CLOUD_COLLECTIONS",
                        "version": "0.1",
                        "files": [
                            {
                                "name": "S6A_MW_0__AMR_____20201203T050820_20201203T050936_20201203T104056_0076_002_041_021_EUM__OPE_ND____.SEN6.tar",
                                "path": "S6A",
                                "size": 112640,
                                "time": 1717620499438,
                                "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                                "bucket": "hryeung-ia-podaac-protected",
                                "type": "data"
                            }
                        ]
                    },
                    {
                        "granuleId": "S6A_MW_2__AMR_____20211030T235312_20211031T004925_20230216T203131_3373_036_008_004_EUM__REP_NT_F08.SEN6.tar",
                        "dataType": "MASTER_S6_CLOUD_COLLECTIONS",
                        "version": "0.1",
                        "files": [
                            {
                                "name": "S6A_MW_2__AMR_____20211030T235312_20211031T004925_20230216T203131_3373_036_008_004_EUM__REP_NT_F08.SEN6.tar",
                                "path": "S6A",
                                "size": 2109440,
                                "time": 1717620499438,
                                "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                                "bucket": "hryeung-ia-podaac-protected",
                                "type": "data"
                            }
                        ]
                    }
                ]
            }
        },
        "task_config": {
            "cumulus_meta": "{$.cumulus_meta}",
            "meta": "{$.meta}"
        }
    }
}

lambda_output = {
    "cumulus_meta": {
        "cumulus_meta_dummy_key": "dummy_value"
    },
    "exception": "None",
    "meta": {
        "collection": {
            "name": "dummy_collection_name"
        }
    },
    "payload": [
        {
            "cma": {
                "task_config": {
                    "provider": "{$.meta.provider}",
                    "provider_path": "{$.meta.collection.meta.provider_path}",
                    "collection": "{$.meta.collection}",
                    "cumulus_meta": "{$.cumulus_meta}",
                    "cumulus_message": ""
                },
                "event": {
                    "cumulus_meta": {
                        "cumulus_meta_dummy_key": "dummy_value"
                    },
                    "meta": {
                        "collection": {
                            "name": "dummy_collection_name"
                        }
                    },
                    "payload": {
                        "granules": [
                            {
                                "granuleId": "S6A_AX____POE__AX_20230524T215923_20230526T015923_20230614T150335__________________SALP_OPE_NT_002.SEN6.tar",
                                "dataType": "MASTER_S6_CLOUD_COLLECTIONS",
                                "version": "0.1",
                                "files": [
                                    {
                                        "name": "S6A_AX____POE__AX_20230524T215923_20230526T015923_20230614T150335__________________SALP_OPE_NT_002.SEN6.tar",
                                        "path": "S6A",
                                        "size": 1024000,
                                        "time": 1717620499438,
                                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                                        "bucket": "hryeung-ia-podaac-protected",
                                        "type": "data"
                                    }
                                ]
                            },
                            {
                                "granuleId": "S6A_DO_0__DOP_____20201203T103532_20201203T123115_20201203T123145_6943_002_047_024_EUM__OPE_NR____.SEN6.tar",
                                "dataType": "MASTER_S6_CLOUD_COLLECTIONS",
                                "version": "0.1",
                                "files": [
                                    {
                                        "name": "S6A_DO_0__DOP_____20201203T103532_20201203T123115_20201203T123145_6943_002_047_024_EUM__OPE_NR____.SEN6.tar",
                                        "path": "S6A",
                                        "size": 1617920,
                                        "time": 1717620499438,
                                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                                        "bucket": "hryeung-ia-podaac-protected",
                                        "type": "data"
                                    }
                                ]
                            },
                            {
                                "granuleId": "S6A_GN_1B_RNXH_AX_20230614T125942_20230614T135941_20230614T153905__________________CPOD_OPE_NR____.SEN6.tar",
                                "dataType": "MASTER_S6_CLOUD_COLLECTIONS",
                                "version": "0.1",
                                "files": [
                                    {
                                        "name": "S6A_GN_1B_RNXH_AX_20230614T125942_20230614T135941_20230614T153905__________________CPOD_OPE_NR____.SEN6.tar",
                                        "path": "S6A",
                                        "size": 1034240,
                                        "time": 1717620499438,
                                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                                        "bucket": "hryeung-ia-podaac-protected",
                                        "type": "data"
                                    }
                                ]
                            },
                            {
                                "granuleId": "S6A_MW_0__AMR_____20201203T050820_20201203T050936_20201203T104056_0076_002_041_021_EUM__OPE_ND____.SEN6.tar",
                                "dataType": "MASTER_S6_CLOUD_COLLECTIONS",
                                "version": "0.1",
                                "files": [
                                    {
                                        "name": "S6A_MW_0__AMR_____20201203T050820_20201203T050936_20201203T104056_0076_002_041_021_EUM__OPE_ND____.SEN6.tar",
                                        "path": "S6A",
                                        "size": 112640,
                                        "time": 1717620499438,
                                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                                        "bucket": "hryeung-ia-podaac-protected",
                                        "type": "data"
                                    }
                                ]
                            },
                            {
                                "granuleId": "S6A_MW_2__AMR_____20211030T235312_20211031T004925_20230216T203131_3373_036_008_004_EUM__REP_NT_F08.SEN6.tar",
                                "dataType": "MASTER_S6_CLOUD_COLLECTIONS",
                                "version": "0.1",
                                "files": [
                                    {
                                        "name": "S6A_MW_2__AMR_____20211030T235312_20211031T004925_20230216T203131_3373_036_008_004_EUM__REP_NT_F08.SEN6.tar",
                                        "path": "S6A",
                                        "size": 2109440,
                                        "time": 1717620499438,
                                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                                        "bucket": "hryeung-ia-podaac-protected",
                                        "type": "data"
                                    }
                                ]
                            }
                        ]
                    },
                    "exception": None
                }
            }
        }
    ],
    "task_config": {
        "cumulus_meta": "{$.cumulus_meta}",
        "meta": "{$.meta}"
    }
}

lambda_output_2 = {
    "cumulus_meta": {
        "cumulus_meta_dummy_key": "dummy_value"
    },
    "exception": "None",
    "meta": {
        "collection": {
            "name": "dummy_collection_name"
        }
    },
    "payload": [
        {
            "cma": {
                "task_config": {
                    "provider": "{$.meta.provider}",
                    "provider_path": "{$.meta.collection.meta.provider_path}",
                    "collection": "{$.meta.collection}",
                    "cumulus_meta": "{$.cumulus_meta}",
                    "cumulus_message": ""
                },
                "event": {
                    "cumulus_meta": {
                        "cumulus_meta_dummy_key": "dummy_value"
                    },
                    "meta": {
                        "collection": {
                            "name": "dummy_collection_name"
                        }
                    },
                    "payload": {
                        "granules": [
                            {
                                "granuleId": "S6A_AX____POE__AX_20230524T215923_20230526T015923_20230614T150335__________________SALP_OPE_NT_002.SEN6.tar",
                                "dataType": "MASTER_S6_CLOUD_COLLECTIONS",
                                "version": "0.1",
                                "files": [
                                    {
                                        "name": "S6A_AX____POE__AX_20230524T215923_20230526T015923_20230614T150335__________________SALP_OPE_NT_002.SEN6.tar",
                                        "path": "S6A",
                                        "size": 1024000,
                                        "time": 1717620499438,
                                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                                        "bucket": "hryeung-ia-podaac-protected",
                                        "type": "data"
                                    }
                                ]
                            },
                            {
                                "granuleId": "S6A_DO_0__DOP_____20201203T103532_20201203T123115_20201203T123145_6943_002_047_024_EUM__OPE_NR____.SEN6.tar",
                                "dataType": "MASTER_S6_CLOUD_COLLECTIONS",
                                "version": "0.1",
                                "files": [
                                    {
                                        "name": "S6A_DO_0__DOP_____20201203T103532_20201203T123115_20201203T123145_6943_002_047_024_EUM__OPE_NR____.SEN6.tar",
                                        "path": "S6A",
                                        "size": 1617920,
                                        "time": 1717620499438,
                                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                                        "bucket": "hryeung-ia-podaac-protected",
                                        "type": "data"
                                    }
                                ]
                            }
                        ]
                    },
                    "exception": None
                }
            }
        },
        {
            "cma": {
                "task_config": {
                    "provider": "{$.meta.provider}",
                    "provider_path": "{$.meta.collection.meta.provider_path}",
                    "collection": "{$.meta.collection}",
                    "cumulus_meta": "{$.cumulus_meta}",
                    "cumulus_message": ""
                },
                "event": {
                    "cumulus_meta": {
                        "cumulus_meta_dummy_key": "dummy_value"
                    },
                    "meta": {
                        "collection": {
                            "name": "dummy_collection_name"
                        }
                    },
                    "payload": {
                        "granules": [
                            {
                                "granuleId": "S6A_GN_1B_RNXH_AX_20230614T125942_20230614T135941_20230614T153905__________________CPOD_OPE_NR____.SEN6.tar",
                                "dataType": "MASTER_S6_CLOUD_COLLECTIONS",
                                "version": "0.1",
                                "files": [
                                    {
                                        "name": "S6A_GN_1B_RNXH_AX_20230614T125942_20230614T135941_20230614T153905__________________CPOD_OPE_NR____.SEN6.tar",
                                        "path": "S6A",
                                        "size": 1034240,
                                        "time": 1717620499438,
                                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                                        "bucket": "hryeung-ia-podaac-protected",
                                        "type": "data"
                                    }
                                ]
                            },
                            {
                                "granuleId": "S6A_MW_0__AMR_____20201203T050820_20201203T050936_20201203T104056_0076_002_041_021_EUM__OPE_ND____.SEN6.tar",
                                "dataType": "MASTER_S6_CLOUD_COLLECTIONS",
                                "version": "0.1",
                                "files": [
                                    {
                                        "name": "S6A_MW_0__AMR_____20201203T050820_20201203T050936_20201203T104056_0076_002_041_021_EUM__OPE_ND____.SEN6.tar",
                                        "path": "S6A",
                                        "size": 112640,
                                        "time": 1717620499438,
                                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                                        "bucket": "hryeung-ia-podaac-protected",
                                        "type": "data"
                                    }
                                ]
                            }
                        ]
                    },
                    "exception": None
                }
            }
        },
        {
            "cma": {
                "task_config": {
                    "provider": "{$.meta.provider}",
                    "provider_path": "{$.meta.collection.meta.provider_path}",
                    "collection": "{$.meta.collection}",
                    "cumulus_meta": "{$.cumulus_meta}",
                    "cumulus_message": ""
                },
                "event": {
                    "cumulus_meta": {
                        "cumulus_meta_dummy_key": "dummy_value"
                    },
                    "meta": {
                        "collection": {
                            "name": "dummy_collection_name"
                        }
                    },
                    "payload": {
                        "granules": [
                            {
                                "granuleId": "S6A_MW_2__AMR_____20211030T235312_20211031T004925_20230216T203131_3373_036_008_004_EUM__REP_NT_F08.SEN6.tar",
                                "dataType": "MASTER_S6_CLOUD_COLLECTIONS",
                                "version": "0.1",
                                "files": [
                                    {
                                        "name": "S6A_MW_2__AMR_____20211030T235312_20211031T004925_20230216T203131_3373_036_008_004_EUM__REP_NT_F08.SEN6.tar",
                                        "path": "S6A",
                                        "size": 2109440,
                                        "time": 1717620499438,
                                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                                        "bucket": "hryeung-ia-podaac-protected",
                                        "type": "data"
                                    }
                                ]
                            }
                        ]
                    },
                    "exception": None
                }
            }
        }
    ],
    "task_config": {
        "cumulus_meta": "{$.cumulus_meta}",
        "meta": "{$.meta}"
    }
}

lambda_input_replace = {
    "cma": {
        "event": {
            "cumulus_meta": {
                "cumulus_version": "9.9.0",
                "message_source": "sfn",
                "system_bucket": "dummy_bucket"
            },
            "replace": {
                "Bucket": "dummy_bucket",
                "Key": "events/dummy_aws_s3_object.json",
                "TargetPath": "$"
            }
        },
        "task_config": {
            "cumulus_meta": "{$.cumulus_meta}",
            "meta": "{$.meta}"
        }
    }
}

s3_file_content = {
    "cumulus_meta": {
        "cumulus_version": "9.9.0",
        "message_source": "sfn",
        "system_bucket": "dummy_bucket"
    },
    "exception": "None",
    "meta": {
        "collection": {
            "name": "VIIRS_NPP-NAVO-L2P-v3.0",
            "meta": {
                "provider_path": "/cumulus-test/gds2/NAVO/"
            }
        },
        "provider": {
            "globalConnectionLimit": 1,
            "host": "sftp_encpoint",
            "id": "podaac-test-sftp",
            "password": "dummy-password",
            "protocol": "sftp",
            "username": "dummy-username"
        },
        "provider_path": "/cumulus-test/gds2/NAVO/"
    },
    "payload": {
        "granules": [
            {
                "granuleId": "20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "dataType": "VIIRS_NPP-NAVO-L2P-v3.0",
                "version": "3.0",
                "files": [
                    {
                        "name": "20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 18167706,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-protected",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "data"
                    },
                    {
                        "name": "20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 97,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-public",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "metadata"
                    }
                ]
            },
            {
                "granuleId": "20220111135133-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "dataType": "VIIRS_NPP-NAVO-L2P-v3.0",
                "version": "3.0",
                "files": [
                    {
                        "name": "20220111135133-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 18294159,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-protected",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "data"
                    },
                    {
                        "name": "20220111135133-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 97,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-public",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "metadata"
                    }
                ]
            },
            {
                "granuleId": "20220111135258-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "dataType": "VIIRS_NPP-NAVO-L2P-v3.0",
                "version": "3.0",
                "files": [
                    {
                        "name": "20220111135258-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 17146221,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-protected",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "data"
                    },
                    {
                        "name": "20220111135258-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 97,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-public",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "metadata"
                    }
                ]
            },
            {
                "granuleId": "20220111135423-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "dataType": "VIIRS_NPP-NAVO-L2P-v3.0",
                "version": "3.0",
                "files": [
                    {
                        "name": "20220111135423-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 18654568,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-protected",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "data"
                    },
                    {
                        "name": "20220111135423-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 97,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-public",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "metadata"
                    }
                ]
            },
            {
                "granuleId": "20220111135549-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "dataType": "VIIRS_NPP-NAVO-L2P-v3.0",
                "version": "3.0",
                "files": [
                    {
                        "name": "20220111135549-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 17730761,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-protected",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "data"
                    },
                    {
                        "name": "20220111135549-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 97,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-public",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "metadata"
                    }
                ]
            },
            {
                "granuleId": "20220111135714-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "dataType": "VIIRS_NPP-NAVO-L2P-v3.0",
                "version": "3.0",
                "files": [
                    {
                        "name": "20220111135714-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 16983663,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-protected",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "data"
                    },
                    {
                        "name": "20220111135714-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 97,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-public",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "metadata"
                    }
                ]
            },
            {
                "granuleId": "20220111135840-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "dataType": "VIIRS_NPP-NAVO-L2P-v3.0",
                "version": "3.0",
                "files": [
                    {
                        "name": "20220111135840-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 16316733,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-protected",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "data"
                    },
                    {
                        "name": "20220111135840-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 97,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-public",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "metadata"
                    }
                ]
            }
        ]
    }
}


def test_basic_input_output_group_by_default_5():
    os.environ['granules_per_group'] = '5'
    resp = lambda_handler(lambda_input, {})
    assert resp == lambda_output


def test_basic_input_output_group_by_2():
    os.environ['granules_per_group'] = '2'
    resp = lambda_handler(lambda_input, {})
    assert resp == lambda_output_2


@mock_s3
def test_input_with_replace():
    # Fake aws s3 bucket
    s3_client = boto3.client('s3', region_name='us-east-1')  # s3 doesn't like us-west-2...
    test_bucket_name = 'dummy_bucket'
    test_bucket_key = 'events/dummy_aws_s3_object.json'
    s3_client.create_bucket(Bucket=test_bucket_name)
    s3_client.put_object(Body=json.dumps(s3_file_content), Bucket=test_bucket_name, Key=test_bucket_key)

    os.environ['granules_per_group'] = '5'

    response = {}
    try:
        response = lambda_handler(lambda_input_replace, {})
    except Exception as e:
        print(e)

    assert len(response['payload']) is 2
    assert len(response['payload'][0]['cma']['event']['payload']['granules']) is 5
    assert len(response['payload'][1]['cma']['event']['payload']['granules']) is 2
    assert 'cumulus_meta' in response['payload'][0]['cma']['event']
    assert 'meta' in response['payload'][0]['cma']['event']

