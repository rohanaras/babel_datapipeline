# babel_datapipeline
Data processing pipeline for Babel

### Command to run
`luigi --module babel_datapipeline tasks.[task_file].[specific_task] --local-scheduler --date [yyyy-mm-dd]`

This is the command to run locally. `[task_file]` and `[specific_task]` refer to the tasks listed in Current Tasks. 

### Current Tasks

**Infomap**
- PajekFactory
- InfomapTask

**IO**
- LocalTargetInputs
- AminerS3Targets
- DynamoOutputTask
- *Future dataset S3 targets would go here*

**Parsers**
- AMinerParse
- *Future dataset parser tasks go here*

**Recommenders**
- CocitationTask
- BibcoupleTask
- EFTask

### Configuration
Luigi specific configuration as described [here](http://luigi.readthedocs.org/en/stable/configuration.html) can be found in configs/luigi.cfg. Dataset configuration can be found in configs/default.cfg. 

### Output files
Currently the datapipeline dumps the outputs of various steps of the pipeline in the folder in which the command is run. In doing so it will create folders named `citation_dict`, `infomap_output`, `pajek_files`, and `recs`. 

### Requirements
This module, in addition to the packages noted in the setup.py and requirements.txt files, requires that [Infomap](http://www.mapequation.org/code.html#Installation) is installed and included in PATH. Additionally, in order to use any of the AWS IO tasks, [boto3 credentials](http://boto3.readthedocs.org/en/latest/guide/configuration.html) must be configured. So far `region`, `aws_access_key_id`, and `aws_secret_access_key` are required.

