---
myst:
  html_meta:
    description: "Read and write custom file types in Ray Data by implementing your own datasource for formats the built-in readers don't cover."
---

(custom_datasource)=

# Advanced: Read and write custom file types

This advanced guide shows you how to extend Ray Data to read and write file types that it doesn't support natively. The guide relies on unstable internal APIs.

Ray Data already supports images with the {func}`~ray.data.read_images` and {meth}`~ray.data.Dataset.write_images` APIs. This example shows you how to implement them anyway, as an illustration.

## Read data from files

:::{tip}
If you're not contributing to Ray Data, you don't need to create a {class}`~ray.data.Datasource`. Instead, call {func}`~ray.data.read_binary_files` and decode files with {meth}`~ray.data.Dataset.map`.
:::

The core abstraction for reading files is {class}`~ray.data.datasource.FileBasedDatasource`. It provides file-specific behavior on top of the {class}`~ray.data.Datasource` interface.

To subclass {class}`~ray.data.datasource.FileBasedDatasource`, implement the constructor and `_read_stream`.

### Implement the constructor

Call the superclass constructor and specify the files you want to read. Optionally, specify valid file extensions. Ray Data ignores files with other extensions.

```{literalinclude} doc_code/custom_datasource_example.py
:language: python
:start-after: __datasource_constructor_start__
:end-before: __datasource_constructor_end__
```

### Implement `_read_stream`

`_read_stream` is a generator that yields one or more blocks of data from a file.

[Blocks](https://github.com/ray-project/ray/blob/23d3bfcb9dd97ea666b7b4b389f29b9cc0810121/python/ray/data/block.py#L54) are Ray Data's internal abstraction for a collection of rows. They can be PyArrow tables, pandas DataFrames, or dictionaries of NumPy arrays.

Don't create a block directly. Instead, add rows of data to a [DelegatingBlockBuilder](https://github.com/ray-project/ray/blob/23d3bfcb9dd97ea666b7b4b389f29b9cc0810121/python/ray/data/_internal/delegating_block_builder.py#L10).

```{literalinclude} doc_code/custom_datasource_example.py
:language: python
:start-after: __read_stream_start__
:end-before: __read_stream_end__
```

### Read your data

After you implement `ImageDatasource`, call {func}`~ray.data.read_datasource` to read images into a {class}`~ray.data.Dataset`. Ray Data reads your files in parallel.

```{literalinclude} doc_code/custom_datasource_example.py
:language: python
:start-after: __read_datasource_start__
:end-before: __read_datasource_end__
```

## Write data to files

:::{note}
The write interface is under active development and might change. To request a feature, [open a GitHub issue](https://github.com/ray-project/ray/issues/new?assignees=&labels=enhancement%2Ctriage&projects=&template=feature-request.yml&title=%5B%3CRay+component%3A+Core%7CRLlib%7Cetc...%3E%5D+).
:::

The core abstractions for writing data to files are {class}`~ray.data.datasource.RowBasedFileDatasink` and {class}`~ray.data.datasource.BlockBasedFileDatasink`. They provide file-specific behavior on top of the {class}`~ray.data.Datasink` interface.

To write one row per file, subclass {class}`~ray.data.datasource.RowBasedFileDatasink`. Otherwise, subclass {class}`~ray.data.datasource.BlockBasedFileDatasink`.

This example writes one image per file, so it subclasses {class}`~ray.data.datasource.RowBasedFileDatasink`. To subclass {class}`~ray.data.datasource.RowBasedFileDatasink`, implement the constructor and {meth}`~ray.data.datasource.RowBasedFileDatasink.write_row_to_file`.

### Implement the constructor

Call the superclass constructor and specify the folder to write to. Optionally, specify a string for the file format, such as `"png"`. Ray Data uses the file format as the file extension.

```{literalinclude} doc_code/custom_datasource_example.py
:language: python
:start-after: __datasink_constructor_start__
:end-before: __datasink_constructor_end__
```

### Implement `write_row_to_file`

`write_row_to_file` writes a row of data to a file. Each row is a dictionary that maps column names to values.

```{literalinclude} doc_code/custom_datasource_example.py
:language: python
:start-after: __write_row_to_file_start__
:end-before: __write_row_to_file_end__
```

### Write your data

After you implement `ImageDatasink`, call {meth}`~ray.data.Dataset.write_datasink` to write images to files. Ray Data writes to multiple files in parallel.

```{literalinclude} doc_code/custom_datasource_example.py
:language: python
:start-after: __write_datasink_start__
:end-before: __write_datasink_end__
```
