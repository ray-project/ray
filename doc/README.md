# Ray Documentation

To compile the documentation, run the following commands from this directory.
Note that Ray must be installed first.

```bash
pip install -r requirements-doc.txt
pip install -U -r requirements-rtd.txt # important for reproducing the deployment environment
make html
open _build/html/index.html
```

To test if there are any build errors with the documentation, do the following.

```bash
sphinx-build -W -b html -d _build/doctrees source _build/html
```

To check if there are broken links, run the following (we are currently not running this
in the CI since there are false positives).

```bash
make linkcheck
```
