# About

## Files 
V6 script split into 2 files

func.py - all functions at begining of TIPPv6

run.py - code after functions in TIPPv6


## Changes 
No code in func.py has been changed

run.py changes
- reused code blocks converted to functions
- data processing moved to functions


## Recommendations
- Use lookup tables for renaming columns, etc.
- Breakup code into scripts by function (i.e. data formating, data validation)
- Try to use functions for repeatable code 
- Don't hardcode 
``` python
    # For example
    
    # Bad
    1 * 2

    # Good
    x = 1
    y = 2
    def compute(x,y):
        return x * y 
```