import copy
from typing import Dict, List, Tuple

import torch
import transformers

import ray


def extract_tensors(m: torch.nn.Module) -> Tuple[torch.nn.Module, List[Dict]]:
    """
    Remove the tensors from a PyTorch model, convert them to NumPy
    arrays, and return the stripped model and tensors.
    """
    tensors = []
    for _, module in m.named_modules():
        # Store the tensors in Python dictionaries
        params = {
            name: torch.clone(param).detach().numpy()
            for name, param in module.named_parameters(recurse=False)
        }
        buffers = {
            name: torch.clone(buf).detach().numpy()
            for name, buf in module.named_buffers(recurse=False)
        }
        tensors.append({"params": params, "buffers": buffers})

    # Make a copy of the original model and strip all tensors and
    # buffers out of the copy.
    m_copy = copy.deepcopy(m)
    for _, module in m_copy.named_modules():
        for name in ([name for name, _ in module.named_parameters(recurse=False)]
                     + [name for name, _ in module.named_buffers(recurse=False)]):
            setattr(module, name, None)

    # Make sure the copy is configured for inference.
    m_copy.train(False)
    return m_copy, tensors


def replace_tensors(m: torch.nn.Module, tensors: List[Dict]):
    """
    Restore the tensors that extract_tensors() stripped out of a
    PyTorch model.
    :param no_parameters_objects: Skip wrapping tensors in
     ``torch.nn.Parameters`` objects (~20% speedup, may impact
     some models)
    """
    modules = [module for _, module in m.named_modules()]
    for module, tensor_dict in zip(modules, tensors):
        # There are separate APIs to set parameters and buffers.
        for name, array in tensor_dict["params"].items():
            module.register_parameter(name,  torch.nn.Parameter(torch.as_tensor(array)))
        for name, array in tensor_dict["buffers"].items():
            module.register_buffer(name, torch.as_tensor(array))


bert = transformers.BertModel.from_pretrained("bert-base-uncased")

ray.init()
bert_skeleton, bert_weights = extract_tensors(bert)

# Load tensors into the model's graph of Python objects
replace_tensors(bert_skeleton, bert_weights)

# Preprocess an example input string for BERT.
test_text = "All work and no play makes Jack a dull boy."
tokenizer = transformers.BertTokenizerFast.from_pretrained(
    "bert-base-uncased")
test_tokens = tokenizer(test_text, return_tensors="pt")

# Run the original model and the copy that we just loaded
print("Original model's output:")
print(bert(**test_tokens).last_hidden_state)
print("\nModel output after zero-copy model loading:")
print(bert_skeleton(**test_tokens).last_hidden_state)
