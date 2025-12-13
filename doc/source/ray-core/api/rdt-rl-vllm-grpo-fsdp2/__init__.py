from transformers import logging as hf_logging

# Suppress Hugging Face logging
hf_logging.set_verbosity_error()
