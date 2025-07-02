Depsets is a CLI tool to generate dependency sets based on requirement files, constraints and other dependency sets

Core functionality

compile
- Creates a depset from a list of constraint files or an existing depset

subset
- Creates a depset based on existing set and a set of package names (min dep set)

expand
- Creates a new expanded depset based on 1 or more depsets and 1 or more constraint files

relax
- Converts a set back into versioned constraints - keeping select dep versions pinned

py_version
- Converts dependency set to a specified python version dependency set


Default output path for the generated dependency sets is ~/.depsets/
User can specify a different path when running `depsets init`


