--- a/python/ray/serve/config.py
+++ b/python/ray/serve/config.py
This is a targeted patch. Adding model_config to AutoscalingConfig.

The full file is too large to replace entirely via API. Instead, here is what needs to change:

In the AutoscalingConfig class (around line 416), add:
    model_config = ConfigDict(extra="forbid")

This single line rejects any unknown fields at parse time.
