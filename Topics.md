# Topics 

## Python and Programing in general
- https://www.python.org/dev/peps/pep-0020/
- https://diveintopython3.net/

## SOQL scripts
- Using aliases - shortens names
- reuse queries using variables
- Create new queries in python
---

## Import python files 
### 
add the path in your script
- Not in your project folder:
  - import sys
  - sys.path.append('/path/to/whatever')
  - import name_of_script (without .py)
- In your project folder
  - import folder_name.script_name (without .py)
---
## Insert data in SQL database and tables
### Oracle 
- https://www.oracletutorial.com/python-oracle/inserting-data/
- https://stackoverflow.com/questions/55825697/how-can-i-append-dataframe-from-pandas-to-the-oracle-table/55826232
### MySQL
- SQLalchemy https://www.sqlalchemy.org/


---
## Package code files / Structuring a repository
### Docs https://docs.python-guide.org/writing/structure/
1. Create Repo in GitHub
-- add README.md
-- add .gitignore

1. Clone Repo to local

```git clone - https://github.com/YOUR-USER-NAME/YOUR-REPO-NAME.git```

2. create virtualenv 

```virtualenv --python python3 venv```

3. activate

```source venv/bin/activate```
- add venv/ to gitignore

4. Open folder in Editor

a. Create new Branch
b. Code
- Add any required packages to requirements.txt
```pip freeze > requirements.txt```
c. Push changes

---
## Procedural documentation