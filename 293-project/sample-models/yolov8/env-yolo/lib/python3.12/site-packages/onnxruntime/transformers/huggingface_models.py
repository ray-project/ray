# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation.  All rights reserved.
# Licensed under the MIT License.  See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

# Maps model class name to a tuple of model class
MODEL_CLASSES = [
    "AutoModel",
    "AutoModelWithLMHead",
    "AutoModelForSequenceClassification",
    "AutoModelForQuestionAnswering",
    "AutoModelForCausalLM",
]

# List of pretrained models: https://huggingface.co/transformers/pretrained_models.html
# Pretrained model name to a tuple of input names, opset_version, use_external_data_format, optimization model type
MODELS = {
    # BERT
    "bert-base-uncased": (
        ["input_ids", "attention_mask", "token_type_ids"],
        12,
        False,
        "bert",
    ),
    "bert-large-uncased": (
        ["input_ids", "attention_mask", "token_type_ids"],
        12,
        False,
        "bert",
    ),
    "bert-base-cased": (
        ["input_ids", "attention_mask", "token_type_ids"],
        12,
        False,
        "bert",
    ),
    # "bert-large-cased": (["input_ids", "attention_mask", "token_type_ids"], 12, False, "bert"),
    # "bert-base-multilingual-uncased": (["input_ids", "attention_mask", "token_type_ids"], 12, False, "bert"),
    # "bert-base-multilingual-cased": (["input_ids", "attention_mask", "token_type_ids"], 12, False, "bert"),
    # "bert-base-chinese": (["input_ids", "attention_mask", "token_type_ids"], 12, False, "bert"),
    # "bert-base-german-cased": (["input_ids", "attention_mask", "token_type_ids"], 12, False, "bert"),
    # "bert-large-uncased-whole-word-masking": (["input_ids", "attention_mask", "token_type_ids"], 12, False, "bert"),
    # "bert-large-cased-whole-word-masking": (["input_ids", "attention_mask", "token_type_ids"], 12, False, "bert"),
    # "bert-large-uncased-whole-word-masking-finetuned-squad": (["input_ids", "attention_mask",
    #                                                            "token_type_ids"], 12, False, "bert"),
    # "bert-large-cased-whole-word-masking-finetuned-squad": (["input_ids", "attention_mask",
    #                                                          "token_type_ids"], 12, False, "bert"),
    # "bert-base-cased-finetuned-mrpc": (["input_ids", "attention_mask", "token_type_ids"], 12, False, "bert"),
    # "bert-base-german-dbmdz-cased": (["input_ids", "attention_mask", "token_type_ids"], 12, False, "bert"),
    # "bert-base-german-dbmdz-uncased": (["input_ids", "attention_mask", "token_type_ids"], 12, False, "bert"),
    # todo: more models to add
    # GPT (no past state)
    "openai-gpt": (["input_ids"], 11, False, "gpt2"),
    # GPT-2 (no past state, use benchmark_gpt2.py for past_key_values)
    "gpt2": (["input_ids"], 11, False, "gpt2"),
    "gpt2-medium": (["input_ids"], 11, False, "gpt2"),
    "gpt2-large": (["input_ids"], 11, True, "gpt2"),
    "gpt2-xl": (["input_ids"], 11, True, "gpt2"),
    "distilgpt2": (["input_ids"], 11, False, "gpt2"),
    # Transformer-XL (Models uses Einsum, which need opset version 12 or later.)
    "transfo-xl-wt103": (["input_ids", "mems"], 12, False, "bert"),
    # XLNet
    "xlnet-base-cased": (["input_ids"], 12, False, "bert"),
    "xlnet-large-cased": (["input_ids"], 12, False, "bert"),
    # XLM
    "xlm-mlm-en-2048": (["input_ids"], 11, True, "bert"),
    "xlm-mlm-ende-1024": (["input_ids"], 11, False, "bert"),
    "xlm-mlm-enfr-1024": (["input_ids"], 11, False, "bert"),
    # RoBERTa
    "roberta-base": (["input_ids", "attention_mask"], 12, False, "bert"),
    "roberta-large": (["input_ids", "attention_mask"], 12, False, "bert"),
    "roberta-large-mnli": (["input_ids", "attention_mask"], 12, False, "bert"),
    "deepset/roberta-base-squad2": (["input_ids", "attention_mask"], 11, False, "bert"),
    "distilroberta-base": (["input_ids", "attention_mask"], 12, False, "bert"),
    # DistilBERT
    "distilbert-base-uncased": (["input_ids", "attention_mask"], 11, False, "bert"),
    "distilbert-base-uncased-distilled-squad": (
        ["input_ids", "attention_mask"],
        11,
        False,
        "bert",
    ),
    # CTRL
    "ctrl": (["input_ids"], 11, True, "bert"),
    # CamemBERT
    "camembert-base": (["input_ids"], 11, False, "bert"),
    # ALBERT
    "albert-base-v1": (["input_ids"], 12, False, "bert"),
    "albert-large-v1": (["input_ids"], 12, False, "bert"),
    "albert-xlarge-v1": (["input_ids"], 12, True, "bert"),
    # "albert-xxlarge-v1": (["input_ids"], 12, True, "bert"),
    "albert-base-v2": (["input_ids"], 12, False, "bert"),
    "albert-large-v2": (["input_ids"], 12, False, "bert"),
    "albert-xlarge-v2": (["input_ids"], 12, True, "bert"),
    # "albert-xxlarge-v2": (["input_ids"], 12, True, "bert"),
    # T5 (use benchmark_t5.py instead)
    # "t5-small": (["input_ids", "decoder_input_ids"], 12, False, "bert"),
    # "t5-base": (["input_ids", "decoder_input_ids"], 12, False, "bert"),
    # "t5-large": (["input_ids", "decoder_input_ids"], 12, True, "bert"),
    # "t5-3b": (["input_ids", "decoder_input_ids"], 12, True, "bert"),
    # "t5-11b": (["input_ids", "decoder_input_ids"], 12, True, "bert"),
    # "valhalla/t5-small-qa-qg-hl": (["input_ids"], 12, True, "bert"),
    # XLM-RoBERTa
    "xlm-roberta-base": (["input_ids"], 11, False, "bert"),
    "xlm-roberta-large": (["input_ids"], 11, True, "bert"),
    # FlauBERT
    "flaubert/flaubert_small_cased": (["input_ids"], 11, False, "bert"),
    # "flaubert/flaubert_base_uncased": (["input_ids"], 11, False, "bert"),
    "flaubert/flaubert_base_cased": (["input_ids"], 11, False, "bert"),
    # "flaubert/flaubert_large_cased": (["input_ids"], 11, False, "bert"),
    # Bart
    "facebook/bart-large": (["input_ids", "attention_mask"], 11, False, "bart"),
    "facebook/bart-base": (["input_ids", "attention_mask"], 11, False, "bart"),
    "facebook/bart-large-mnli": (["input_ids", "attention_mask"], 11, False, "bart"),
    "facebook/bart-large-cnn": (["input_ids", "attention_mask"], 11, False, "bart"),
    # DialoGPT
    "microsoft/DialoGPT-small": (["input_ids"], 11, False, "gpt2"),
    "microsoft/DialoGPT-medium": (["input_ids"], 11, False, "gpt2"),
    # "microsoft/DialoGPT-large": (["input_ids"], 11, True, "gpt2"),
    # Reformer
    # "google/reformer-enwik8": (["input_ids"], 11, False, "bert"),
    # "google/reformer-crime-and-punishment": (["input_ids"], 11, False, "bert"),
    # MarianMT
    # "Helsinki-NLP/opus-mt-ROMANCE-en": (["input_ids"], 12, False, "bert"),
    # Longformer (use benchmark_longformer.py instead)
    # "allenai/longformer-base-4096": (["input_ids"], 12, False, "bert"),
    # "allenai/longformer-large-4096": (["input_ids"], 12, False, "bert"),
    # MBart
    "facebook/mbart-large-cc25": (["input_ids"], 11, True, "bert"),
    "facebook/mbart-large-en-ro": (["input_ids"], 11, True, "bert"),
    # "Helsinki-NLP/opus-mt-ROMANCE-en": (["input_ids"], 12, False, "bert"),
    # # Longformer
    # "allenai/longformer-base-4096": (["input_ids"], 12, False, "bert"),
    # "allenai/longformer-large-4096": (["input_ids"], 12, True, "bert"),
    # "funnel-transformer/small": (["input_ids"], 12, False, "bert"),
    # "funnel-transformer/small-base": (["input_ids"], 12, False, "bert"),
    # "funnel-transformer/medium": (["input_ids"], 12, False, "bert"),
    # "funnel-transformer/medium-base": (["input_ids"], 12, False, "bert"),
    # "funnel-transformer/intermediate": (["input_ids"], 12, False, "bert"),
    # "funnel-transformer/intermediate-base": (["input_ids"], 12, False, "bert"),
    # "funnel-transformer/large": (["input_ids"], 12, True, "bert"),
    # "funnel-transformer/large-base": (["input_ids"], 12, True, "bert"),
    # "funnel-transformer/xlarge": (["input_ids"], 12, True, "bert"),
    # "funnel-transformer/xlarge-base": (["input_ids"], 12, True, "bert"),
    # Layoutlm
    "microsoft/layoutlm-base-uncased": (["input_ids"], 11, False, "bert"),
    "microsoft/layoutlm-large-uncased": (["input_ids"], 11, False, "bert"),
    # Squeezebert
    "squeezebert/squeezebert-uncased": (["input_ids"], 11, False, "bert"),
    "squeezebert/squeezebert-mnli": (["input_ids"], 11, False, "bert"),
    "squeezebert/squeezebert-mnli-headless": (["input_ids"], 11, False, "bert"),
    "unc-nlp/lxmert-base-uncased": (
        ["input_ids", "visual_feats", "visual_pos"],
        11,
        False,
        "bert",
    ),
    # "google/pegasus-xsum": (["input_ids"], 11, False, "bert"),
    # "google/pegasus-large": (["input_ids"], 11, False, "bert"),
    # ViT
    "google/vit-base-patch16-224": (["pixel_values"], 12, False, "vit"),
    # Swin
    "microsoft/swin-base-patch4-window7-224": (["pixel_values"], 12, False, "swin"),
    "microsoft/swin-small-patch4-window7-224": (["pixel_values"], 12, False, "swin"),
    "microsoft/swin-tiny-patch4-window7-224": (["pixel_values"], 12, False, "swin"),
}
