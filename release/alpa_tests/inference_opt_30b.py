import argparse
from transformers import AutoTokenizer
from llm_serving.model.wrapper import get_alpa_model


def get_model_and_tokenizer(args):
    tokenizer = AutoTokenizer.from_pretrained(args.model_dir)
    tokenizer.add_bos_token = False

    model = get_alpa_model(
        model_name="alpa/opt-30b",
        path=args.model_dir,
        batch_size=args.batch_size,
    )

    return model, tokenizer


def run(args):
    model, tokenizer = get_model_and_tokenizer(args)

    # A batch of prompts.
    prompts = ["Paris is such a beautiful city that"] * args.batch_size

    # Generation.
    encoded = tokenizer(prompts, return_tensors="pt")
    output = model.generate(**encoded, max_length=args.max_length, do_sample=True)
    decoded = tokenizer.batch_decode(output, skip_special_tokens=True)

    for response in decoded:
        print(response)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--model_dir",
        type=str,
        default=None,
        required=True,
        help="Path to pretrained JAX model weights.",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=4,
        help="Inference batch size.",
    )
    parser.add_argument(
        "--max_length",
        type=int,
        default=256,
        help="Maximum number of tokens to generate.",
    )

    args = parser.parse_args()

    run(args)
