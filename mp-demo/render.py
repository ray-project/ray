import argparse
# import multiprocessing
from ray.experimental import multiprocessing
import os
import subprocess
import tempfile

os.environ["RAY_ADDRESS"] = "auto"
os.environ["RAY_FORCE_DIRECT"] = "1"

latex_template = r"""
\documentclass[border=2pt]{standalone}
\usepackage{amsmath}
\begin{document}
$
\displaystyle
%s
$
\end{document}
"""

def render(args):
    index, formula, output_dir = args
    with tempfile.TemporaryDirectory() as temp_dir:
        base = "{:06}".format(index)
        latex_path = os.path.join(temp_dir, "{}.tex".format(base))
        pdf_path = os.path.join(temp_dir, "{}.pdf".format(base))
        png_path = os.path.join(output_dir, "{}.png".format(base))
        os.makedirs(output_dir, exist_ok=True)
        with open(latex_path, "w") as latex_file:
            latex_file.write(latex_template % formula)
        try:
            subprocess.check_output([
                "pdflatex",
                "-halt-on-error",
                "-interaction=nonstopmode",
                "-output-directory={}".format(temp_dir),
                latex_path
            ], stderr=subprocess.STDOUT)
            subprocess.check_output([
                "convert",
                "-density", "200",
                "-quality", "100",
                pdf_path,
                png_path
            ], stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            return None
    return index, png_path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--formulas-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--num-formulas", default=100000, type=int)
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    formulas = []
    with open(args.formulas_path) as infile:
        for i, line in enumerate(infile):
            if i == args.num_formulas:
                break
            formulas.append(line.strip())

    jobs = [
        (index, formula, args.output_dir)
        for index, formula in enumerate(formulas)
    ]

    with multiprocessing.Pool() as pool:
        for result in pool.imap_unordered(render, jobs, chunksize=5):
            if result is not None:
                index, png_path = result
                print("Formula {:,} was rendered to {}.".format(index, png_path))

if __name__ == "__main__":
    main()
