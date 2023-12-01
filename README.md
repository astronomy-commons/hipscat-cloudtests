# hipscat_cloudtests

[![Template](https://img.shields.io/badge/Template-LINCC%20Frameworks%20Python%20Project%20Template-brightgreen)](https://lincc-ppt.readthedocs.io/en/latest/)

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/astronomy-commons/hipscat_cloudtests/smoke-test.yml)](https://github.com/astronomy-commons/hipscat_cloudtests/actions/workflows/smoke-test.yml)
[![benchmarks](https://img.shields.io/github/actions/workflow/status/astronomy-commons/hipscat_cloudtests/asv-main.yml?label=benchmarks)](https://astronomy-commons.github.io/hipscat_cloudtests/)

Integration tests for cloud read and write through HiPScat and LSDB libraries.

## Dev Guide - Getting Started

Before installing any dependencies or writing code, it's a great idea to create a
virtual environment. LINCC-Frameworks engineers primarily use `conda` to manage virtual
environments. If you have conda installed locally, you can run the following to
create and activate a new environment.

```
>> conda create env -n <env_name> python=3.10
>> conda activate <env_name>
```

Once you have created a new environment, you can install this project for local
development using the following commands:

```
>> pip install -e .'[dev]'
>> pre-commit install
>> conda install pandoc
```

## Performing HiPSCat cloud tests locally

The only currently implemented cloud platform is abfs. In order to run the tests, you will need to 
export the following environmental variables in a command line:

```bash
export ABFS_LINCCDATA_ACCOUNT_NAME=lincc_account_name
export ABFS_LINCCDATA_ACCOUNT_KEY=lincc_account_key
```

Then to run the tests:

```bash
pytest --cloud abfs
```

### How are we connecting to the cloud resources?

We have abstracted our entire i/o infrastructure to be read through the python 
[fsspec](https://filesystem-spec.readthedocs.io/en/latest/index.html) library. 
All that needs to be provided is a valid protocol pathway, and storage options 
for the cloud interface. 

## Adding tests for a new cloud interface protocol

There are various steps to have tests run on another cloud bucket provider (like s3 or gcs). 

1. You will have to create the container/bucket
2. You will have to edit `tests/conftest.py` in multiple places:

```python
...
#...line 38...
@pytest.fixture
def example_cloud_path(cloud):
    if cloud == "abfs":
        return "abfs://hipscat/pytests/hipscat"
    
    #your new addition
    elif cloud == "new_protocol":
        return "new_protocol://path/to/pytest/hipscat"

    raise NotImplementedError("Cloud format not implemented for hipscat tests!")

@pytest.fixture
def example_cloud_storage_options(cloud):
    if cloud == "abfs":
        storage_options = {
            "account_key" : os.environ.get("ABFS_LINCCDATA_ACCOUNT_KEY"),
            "account_name" : os.environ.get("ABFS_LINCCDATA_ACCOUNT_NAME")
        }
        return storage_options
    
    #your new addition
    elif cloud == "new_protocol":
        storage_options = {
            "valid_storage_option_param1" : os.environ.get("NEW_PROTOCOL_PARAM1"),
            "valid_storage_option_param2" : os.environ.get("NEW_PROTOCOL_PARAM2"),
            ...
        }

    return {}
```

3. Finally, you will need to copy several `/tests/data/` directories into your newly 
   created bucket. This can be accomplished by running the `copy_data_to_fs.py` script.
4. Before running the tests, you will need to export your `valid_storage_option_param` into the environment.


## Adding tests to the github workflows

1. TODO - enumerate these steps
1. REPOSITORY secrets
1. smoke_test.yml
1. testing-and-coverage
