# Database scripts
# ------------------------------------------------------------------------------
## Overview
# ------------------------------------------------------------------------------
- **courses2** folder contains scripts to extract data from tables in portal
- **contacts** folder contains script to extract data from contacts and pathways
- **extract_tables** folder contains script to extract data from all/most tables in portal
- **modules** folder contains scripts to generate database schemas and libraries used by other scripts


`generate_sql.py` in folder `courses2` is used to generate sql queries for one or more tables

`dynamic_extract.py` in folder `courses2` is used to extract data using the generated sql query. It also includes the algorithm to convert row data into hierarchical json format

> note: `dynamic_extract.py` was formerly called `create_view.py`. instances where `create_view.py` is mentioned refers to `dynamic_extract.py` 

`dynamic_insert.py` in folder `courses2` is used to insert data `UNFINISHED`

`generate_schema.py` in folder `modules` is used to generate database schema data. i.e the intermediate files containing the trees

`create_jsons.py` in folder `courses2` is used to specifically extract students, submissions, and answers `DEPRECATED`

`extract_contacts.py` in folder `contacts` is used to extract data from contacts, and other related tables `DEPRECATED`

scripts in folder `extract_tables` are used to extract data from tables in portal. They can also be used to validate data in the database `DEPRECATED`
# ------------------------------------------------------------------------------
## Steps you need to take before running the scripts
# ------------------------------------------------------------------------------
Please ensure that the `psycopg2` python library is intalled before running any scripts.
To install psycopg2, run `pip install psycopg2` on the command line, or follow instructions here: https://www.psycopg.org/docs/install.html

Also remember to set the database credentials in your environment variables. To do this, go to folder `modules`. run `db_info_sample.sh` if you are using a linux or mac operating system. run `db_info_sample.cmd` if you are using a windows operating system.

For linux or mac:
```sh
$ source modules/db_info_sample.sh
```
For windows:
```sh
modules/db_info_sample.cmd
```

## How to perform some operations


> Remember to Run shell script to set environment variables to connect to the database before performing any operations


### Generate schema data json
To generate a fully detailed schema json file follow these steps

- **Step 1** Extract the schema and tables which data needs to be extracted from
To do this run, the following commands. 
> `generate_schema.py` is located in folder `modules`

```sh
python3 generate_schema.py --generate_schema_tables --outfile <output file> (extracts all schemas)
python3 generate_schema.py --generate_schema_tables --schema <specify schemas> --outfile <output file>
```

This generates a file containing the database schemas and their tables

- **Step 2** Use the generated file from the previous step to generate a new file that contains the tables with their trees/dependencies
To do this run, the following commands.

```sh
python3 generate_schema.py --generate_schema --infile <enter generated file from previous step> --outfile <output file>
```

> You may need to make modifications to this file. The constraints assigned to the tables may be incorrect.

- **Step 3** Generate the final detailed schema json
To do this, run the following commands.

```sh
python3 generate_schema.py --generate_detailed_schema --infile <enter output file from previous step> --outfile <output file>
```

### Generate sql for one or more tables
Generate sql using the script `generate_sql.py` found in folder courses2

The schema json generated in `step 3`  is used by this script to determine the relationship between tables

This is how to run the script:
```sh
python3 generate_sql.py --tables <specify table(s)> --schemajson <detailed schema json> --outfile <output file>
```


### Extract data from generated sql
`dynamic_extract.py` is used to execute the generated sql query and extract its data

```sh
python3 dynamic_extract.py --schemajson <generated schema json> --queryfile <generated sql query> --outfile <output file>
``` 

#
#
# Entire process roadmap
*** generate_schema.py -> schema_tables -> generate_schema.py -> schema_json -> generate_schema.py -> detailed_schema_json -> generate_sql.py -> sql_query -> dynamic_extract.py -> data ***
#

> the scripts `generate_schema.py`, `generate_sql.py` and `dynamic_extract` are tested and work correctly.

# ------------------------------------------------------------------------------
# Examples
# ------------------------------------------------------------------------------
#### These are examples of how the scripts should be used to extract data
Files with these examples can also be found in the project's directory.

This is an example showing a script and its commands to generate the schema data and sql query for `portal.sections` 

```sh

    python3 modules/generate_schema.py --generate_schema_tables --schemas portal --outfile schema_tables.json

    python3 modules/generate_schema.py --generate_schema --infile schema_tables.json --outfile intermediate.json

    python3 fix_json.py --intermediate

    python3 modules/generate_schema.py --generate_detailed_schema --infile intermediate.json --outfile detailed.json
    
    python3 fix_json.py --detailed

    python3 courses2/generate_sql.py --outfile output.json --schemajson detailed.json --tables portal.sections
```

You should end up with a two json files named `output.json`(contains the sql query) and `detailed.json`(contains the schema data). Now execute `run_dynamic_extract.py` to extract the data
Simply run:

```sh
python3 run_dynamic_extract.py
```
The extracted data will be stored in a file named: `data.json` 

# ------------------------------------------------------------------------------
# Work In Progess
# ------------------------------------------------------------------------------
The script `dynamic_insert.py` is still in the works. The purpose of the script is to
dynamically reinsert the extracted data into any database.

The script can be found in folder `course2`.

The algorithm to be implemented to complete the script will:
> combine 2 or more foreign/unique keys to resolve a table's identifier

i.e. `paths.prefix` and `lessons.relative_path` should be resolved to `lessons_id` and
`paths.prefix`, `lessons.relative_path`, `activities.form_code` should be resolved to `activities_id`



# ------------------------------------------------------------------------------
# Helpers and other files in folder `modules`
# ------------------------------------------------------------------------------
This section describes the purpose of modules/helpers used in the scripts

`generate_schema.py`: This module generates schema data for table(s)

`connect_to_db.py`: This module establishes a connection to the database and returns a handler

`get_columns.py`: This module performs functions such as; retrieves the columns of a table, retrieves the columns and its data, retrieves the column and exclude arbitrary ids

`schema.py`: This modules performs functions such as; creates a json file containing all tables in a schema, validates two schema by comparing their data

`convert_type.py`: converts NULL values into empty lists and empty lists into NULL values

`create_relationship.py`: (DEPRECATED) This module is incomplete and not used

`insert_data.py`: This module is used to insert data into the database

`Schema_Json`: Hardcoded schema json for answers and its parent/friend tables

`topo_sort.py`: Simple implementation of topological sort




# ------------------------------------------------------------------------------
## Everything below this point is DEPRECATED
# ------------------------------------------------------------------------------

### Extract/insert submissions and answers

Extract and insert answers and submission data using `create_jsons.py` located in `courses2` folder

- **Data extraction usage:**
Extract data using `create_jsons.py` file. Add flags to specify precise data
```sh
python3 create_jsons.py --extract 
python3 create_jsons.py --extract --section vzmpsv
```

- **Data insertion usage:**
Insert data using `create_jsons.py` file. Ensure json file for insertion exists.
Special case: Use `--insert_course` flag to insert answers and submissions into a specified course. Using `--insert_course` flag will insert data into specified course_name
```sh
python3 create_jsons.py --insert --infile file.json
python3 create_jsons.py --insert_course --course course_name --infile file.json
```


### Extract/Insert or duplicate course data
Extract and duplicate course data using `extract_questions.py` located in `extract_tables` folder

> passing in only --extract flag extracts all questions

- **Course data extraction usage:**
Extract data using `extract_questions.py`file. Add flags to specify course_name and section
```sh
python3 extract_questions.py --extract
python3 extract_questions.py --extract_course --course ict/21c --section vzmpsv
```

- **Course data insertion usage:**

> Ensure that the course for data insertion exists in database

Insert data using `extract_questions.py` file. Ensure json file for inertion exists
```sh
python3 extract_questions.py --insert --infile file.json
python3 extract_questions.py --insert_course --course ict/21d --infile file.json
```




### Contacts scripts
Scripts in folder contacts do not work correctly.