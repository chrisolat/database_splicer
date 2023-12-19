## create_view.py
The **create_view.py** script is used to generate sql select statements of passed in table or tables

### Functions
- **generate_sql(table_input)** generate sql of pass in table names. table_input is a list of table names

### Usage
> **Note**: If more than one table is entered, they must be connected. i.e. courses and paths can be entered because courses course_name is a foriegn key in paths. courses and lessons cannot be entered because they are not connected.


> **Note**: Schema data for any table passed in must be present in a file called `Portal_Schema` located in `modules` directory

```sh
python3 create_view.py lessons
python3 create_view.py courses paths lessons
```