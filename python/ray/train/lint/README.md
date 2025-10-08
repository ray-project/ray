## Ray Train Circular Import Linter

Currently, Ray Train functionlity is patched with functionality from other directories in certain cases. For instance, Ray Train is patched with functionality from Ray Train v2 when `RAY_TRAIN_V2_ENABLED=1`. The Ray Train Circular Import Linter takes a `patch_dir` and searches for circular imports between the base Ray Train directory (`ray/python/ray/train`) and the patching directory.

### The Problem:

When there is a circular import present, import errors are triggered by importing directly from a patching directory. For example, directly importing the v2 TensorflowTrainer with `from ray.train.v2.tensorflow.tensorflow_trainer import TensorflowTrainer` rather than relying on the conventional v2 routing logic with `from ray.train.tensorflow import TensorflowTrainer`. This happens in the case of user API misuse and also during the deserialization of the train function on train worker setup. The following image depicts the error path of such erroneous imports.

![ErrorPath](./images/ErrorPath.png)

### The Fix:

To make Ray Train resiliant to such erroneous imports, this linter proactively detects circular imports and specifies how to fix it. The fix perscribed by the linter prevents errors by importing the base train packages early within in the patching directory. In the below example, the previously depicted circular import is resolved by the linter's suggested fix to import `ray.train.foo` early in `ray.train.v2.foo`.

![SuccessPath](./images/SuccessPath.png)

### Linter Mechanics

The linter implements a NodeVisitor to parse imports within the base train directory and the patching directory to detect circular imports. The below example depicts two circular imports detected by the linter originating from a `ray.train.common.__init__.py` file.

![Linter](./images/Linter.png)

The linter first parses all `__init__.py` files in the base train directory and collects their imports. For each import from the patching directory, the linter will also collect the imports from in patching file (e.g. `ray.train.v2.foo.py`) and if any of these imports point back to the same base train file (e.g. `ray.train.common.__init__.py`), a violation is detected.

However, notice from the diagram that the linter can also find violations in the case of reexports. If the base train file points to a package file (e.g. `ray.train.v2.bar.__init__.py`), the linter will also collect the imports of the referenced implementation file (e.g. `ray.train.v2.bar.bar_impl.py`) to search for a violation.

However, in both cases, if the linter finds that the base train file is imported early in the patching package file (e.g. `ray.train.common` is imported in `ray.train.v2.foo.__init__.py`/`ray.train.v2.bar.__init__.py`), then the violation will be suppressed. Otherwise, this is the fix that will be reccommended by the linter.
