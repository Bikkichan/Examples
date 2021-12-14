# Topics 

## Python and Programing in general
https://www.python.org/dev/peps/pep-0020/

## SOQL scripts
---

## Import python files 
### 
add the path in your script
- import sys
- sys.path.append('/path/to/whatever')

---
## Insert data in SQL database and tables
### Oracle https://www.oracletutorial.com/python-oracle/inserting-data/
### SQLalchemy https://www.sqlalchemy.org/

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