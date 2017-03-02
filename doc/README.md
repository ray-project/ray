# Ray Documentation

To compile the documentation, run the following commands from this directory.
Note that Ray must be installed first.

```
pip install -r requirements-doc.txt
make html
open _build/html/index.html
```

To test if there are any build errors with the documentation, do the following.

```
sphinx-build -W -b html -d _build/doctrees source _build/html
```
