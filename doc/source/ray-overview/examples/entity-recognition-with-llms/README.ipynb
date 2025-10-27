{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# LLM training and inference\n",
    "\n",
    "<a href=\"https://console.anyscale.com/register/ha?render_flow=ray&utm_source=ray_docs&utm_medium=docs&utm_campaign=entity-recognition-with-llms&redirectTo=/v2/template-preview/entity-recognition-with-llms\\\">\n",
    "<img src=\"https://raw.githubusercontent.com/ray-project/ray/c34b74c22a9390aa89baf80815ede59397786d2e/doc/source/_static/img/run-on-anyscale.svg\" alt=\\\"Run on Anyscale\\\">\n",
    "</a>\n",
    "<br></br>\n",
    "<div align=\"left\">\n",
    "<a href=\"https://github.com/anyscale/e2e-llm-workflows\" role=\"button\"><img src=\"https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d\"></a>&nbsp;\n",
    "</div>"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "This end-to-end tutorial **fine-tunes** an LLM to perform **batch inference** and **online serving** at scale. While entity recognition (NER) is the main task in this tutorial, you can easily extend these end-to-end workflows to any use case.\n",
    "\n",
    "<img src=\"https://raw.githubusercontent.com/anyscale/e2e-llm-workflows/refs/heads/main/images/e2e_llm.png\" width=800>\n",
    "\n",
    "**Note**: The intent of this tutorial is to show how you can use Ray to implement end-to-end LLM workflows that can extend to any use case, including multimodal.\n",
    "\n",
    "This tutorial uses the [Ray library](https://github.com/ray-project/ray) to implement these workflows, namely the LLM APIs:\n",
    "\n",
    "[`ray.data.llm`](https://docs.ray.io/en/latest/data/working-with-llms.html):\n",
    "- Batch inference over distributed datasets\n",
    "- Streaming and async execution for throughput\n",
    "- Built-in metrics and tracing, including observability\n",
    "- Zero-copy GPU data transfer\n",
    "- Composable with preprocessing and postprocessing steps\n",
    "\n",
    "[`ray.serve.llm`](https://docs.ray.io/en/latest/serve/llm/serving-llms.html):\n",
    "- Automatic scaling and load balancing\n",
    "- Unified multi-node multi-model deployment\n",
    "- Multi-LoRA support with shared base models\n",
    "- Deep integration with inference engines, vLLM to start\n",
    "- Composable multi-model LLM pipelines\n",
    "\n",
    "And all of these workloads come with all the observability views you need to debug and tune them to **maximize throughput/latency**."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Set up\n",
    "\n",
    "### Compute\n",
    "This [Anyscale Workspace](https://docs.anyscale.com/platform/workspaces/) automatically provisions and autoscales the compute your workloads need. If you're not on Anyscale, then you need to provision the appropriate compute (L4) for this tutorial.\n",
    "\n",
    "<img src=\"https://raw.githubusercontent.com/anyscale/foundational-ray-app/refs/heads/main/images/compute.png\" width=500>\n",
    "\n",
    "### Dependencies\n",
    "Start by downloading the dependencies required for this tutorial. Notice in your [`containerfile`](https://raw.githubusercontent.com/anyscale/e2e-llm-workflows/refs/heads/main/containerfile) you have a base image [`anyscale/ray-llm:latest-py311-cu124`](https://hub.docker.com/layers/anyscale/ray-llm/latest-py311-cu124/images/sha256-5a1c55f7f416d2d2eb5f4cdd13afeda25d4f7383406cfee1f1f60da495d1b50f) followed by a list of pip packages. If you're not on [Anyscale](https://console.anyscale.com/), you can pull this Docker image yourself and install the dependencies.\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\u001b[92mSuccessfully registered `ray, vllm` and 5 other packages to be installed on all cluster nodes.\u001b[0m\n",
      "\u001b[92mView and update dependencies here: https://console.anyscale.com/cld_kvedZWag2qA8i5BjxUevf5i7/prj_cz951f43jjdybtzkx1s5sjgz99/workspaces/expwrk_mp8cxvgle2yeumgcpu1yua2r3e?workspace-tab=dependencies\u001b[0m\n"
     ]
    }
   ],
   "source": [
    "%%bash\n",
    "# Install dependencies\n",
    "pip install -q \\\n",
    "    \"xgrammar==0.1.11\" \\\n",
    "    \"pynvml==12.0.0\" \\\n",
    "    \"hf_transfer==0.1.9\" \\\n",
    "    \"tensorboard==2.19.0\" \\\n",
    "    \"llamafactory@git+https://github.com/hiyouga/LLaMA-Factory.git@ac8c6fdd3ab7fb6372f231f238e6b8ba6a17eb16#egg=llamafactory\""
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Data ingestion"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "import json\n",
    "import textwrap\n",
    "from IPython.display import Code, Image, display"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Start by downloading the data from cloud storage to local shared storage. "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "download: s3://viggo-ds/train.jsonl to ../../../mnt/cluster_storage/viggo/train.jsonl\n",
      "download: s3://viggo-ds/val.jsonl to ../../../mnt/cluster_storage/viggo/val.jsonl\n",
      "download: s3://viggo-ds/test.jsonl to ../../../mnt/cluster_storage/viggo/test.jsonl\n",
      "download: s3://viggo-ds/dataset_info.json to ../../../mnt/cluster_storage/viggo/dataset_info.json\n"
     ]
    }
   ],
   "source": [
    "%%bash\n",
    "rm -rf /mnt/cluster_storage/viggo  # clean up\n",
    "mkdir /mnt/cluster_storage/viggo\n",
    "wget https://viggo-ds.s3.amazonaws.com/train.jsonl -O /mnt/cluster_storage/viggo/train.jsonl\n",
    "wget https://viggo-ds.s3.amazonaws.com/val.jsonl -O /mnt/cluster_storage/viggo/val.jsonl\n",
    "wget https://viggo-ds.s3.amazonaws.com/test.jsonl -O /mnt/cluster_storage/viggo/test.jsonl\n",
    "wget https://viggo-ds.s3.amazonaws.com/dataset_info.json -O /mnt/cluster_storage/viggo/dataset_info.json"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "{\n",
      "    \"instruction\": \"Given a target sentence construct the underlying meaning representation of the input sentence as a single function with attributes and attribute values. This function should describe the target string accurately and the function must be one of the following ['inform', 'request', 'give_opinion', 'confirm', 'verify_attribute', 'suggest', 'request_explanation', 'recommend', 'request_attribute']. The attributes must be one of the following: ['name', 'exp_release_date', 'release_year', 'developer', 'esrb', 'rating', 'genres', 'player_perspective', 'has_multiplayer', 'platforms', 'available_on_steam', 'has_linux_release', 'has_mac_release', 'specifier']\",\n",
      "    \"input\": \"Blizzard North is mostly an okay developer, but they released Diablo II for the Mac and so that pushes the game from okay to good in my view.\",\n",
      "    \"output\": \"give_opinion(name[Diablo II], developer[Blizzard North], rating[good], has_mac_release[yes])\"\n",
      "}\n"
     ]
    }
   ],
   "source": [
    "%%bash\n",
    "head -n 1 /mnt/cluster_storage/viggo/train.jsonl | python3 -m json.tool"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Given a target sentence construct the underlying meaning representation of the\n",
      "input sentence as a single function with attributes and attribute values. This\n",
      "function should describe the target string accurately and the function must be\n",
      "one of the following ['inform', 'request', 'give_opinion', 'confirm',\n",
      "'verify_attribute', 'suggest', 'request_explanation', 'recommend',\n",
      "'request_attribute']. The attributes must be one of the following: ['name',\n",
      "'exp_release_date', 'release_year', 'developer', 'esrb', 'rating', 'genres',\n",
      "'player_perspective', 'has_multiplayer', 'platforms', 'available_on_steam',\n",
      "'has_linux_release', 'has_mac_release', 'specifier']\n"
     ]
    }
   ],
   "source": [
    "with open(\"/mnt/cluster_storage/viggo/train.jsonl\", \"r\") as fp:\n",
    "    first_line = fp.readline()\n",
    "    item = json.loads(first_line)\n",
    "system_content = item[\"instruction\"]\n",
    "print(textwrap.fill(system_content, width=80))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "You also have an info file that identifies the datasets and format (Alpaca and ShareGPT formats) to use for post training."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<style>pre { line-height: 125%; }\n",
       "td.linenos .normal { color: inherit; background-color: transparent; padding-left: 5px; padding-right: 5px; }\n",
       "span.linenos { color: inherit; background-color: transparent; padding-left: 5px; padding-right: 5px; }\n",
       "td.linenos .special { color: #000000; background-color: #ffffc0; padding-left: 5px; padding-right: 5px; }\n",
       "span.linenos.special { color: #000000; background-color: #ffffc0; padding-left: 5px; padding-right: 5px; }\n",
       ".output_html .hll { background-color: #ffffcc }\n",
       ".output_html { background: #f8f8f8; }\n",
       ".output_html .c { color: #3D7B7B; font-style: italic } /* Comment */\n",
       ".output_html .err { border: 1px solid #FF0000 } /* Error */\n",
       ".output_html .k { color: #008000; font-weight: bold } /* Keyword */\n",
       ".output_html .o { color: #666666 } /* Operator */\n",
       ".output_html .ch { color: #3D7B7B; font-style: italic } /* Comment.Hashbang */\n",
       ".output_html .cm { color: #3D7B7B; font-style: italic } /* Comment.Multiline */\n",
       ".output_html .cp { color: #9C6500 } /* Comment.Preproc */\n",
       ".output_html .cpf { color: #3D7B7B; font-style: italic } /* Comment.PreprocFile */\n",
       ".output_html .c1 { color: #3D7B7B; font-style: italic } /* Comment.Single */\n",
       ".output_html .cs { color: #3D7B7B; font-style: italic } /* Comment.Special */\n",
       ".output_html .gd { color: #A00000 } /* Generic.Deleted */\n",
       ".output_html .ge { font-style: italic } /* Generic.Emph */\n",
       ".output_html .ges { font-weight: bold; font-style: italic } /* Generic.EmphStrong */\n",
       ".output_html .gr { color: #E40000 } /* Generic.Error */\n",
       ".output_html .gh { color: #000080; font-weight: bold } /* Generic.Heading */\n",
       ".output_html .gi { color: #008400 } /* Generic.Inserted */\n",
       ".output_html .go { color: #717171 } /* Generic.Output */\n",
       ".output_html .gp { color: #000080; font-weight: bold } /* Generic.Prompt */\n",
       ".output_html .gs { font-weight: bold } /* Generic.Strong */\n",
       ".output_html .gu { color: #800080; font-weight: bold } /* Generic.Subheading */\n",
       ".output_html .gt { color: #0044DD } /* Generic.Traceback */\n",
       ".output_html .kc { color: #008000; font-weight: bold } /* Keyword.Constant */\n",
       ".output_html .kd { color: #008000; font-weight: bold } /* Keyword.Declaration */\n",
       ".output_html .kn { color: #008000; font-weight: bold } /* Keyword.Namespace */\n",
       ".output_html .kp { color: #008000 } /* Keyword.Pseudo */\n",
       ".output_html .kr { color: #008000; font-weight: bold } /* Keyword.Reserved */\n",
       ".output_html .kt { color: #B00040 } /* Keyword.Type */\n",
       ".output_html .m { color: #666666 } /* Literal.Number */\n",
       ".output_html .s { color: #BA2121 } /* Literal.String */\n",
       ".output_html .na { color: #687822 } /* Name.Attribute */\n",
       ".output_html .nb { color: #008000 } /* Name.Builtin */\n",
       ".output_html .nc { color: #0000FF; font-weight: bold } /* Name.Class */\n",
       ".output_html .no { color: #880000 } /* Name.Constant */\n",
       ".output_html .nd { color: #AA22FF } /* Name.Decorator */\n",
       ".output_html .ni { color: #717171; font-weight: bold } /* Name.Entity */\n",
       ".output_html .ne { color: #CB3F38; font-weight: bold } /* Name.Exception */\n",
       ".output_html .nf { color: #0000FF } /* Name.Function */\n",
       ".output_html .nl { color: #767600 } /* Name.Label */\n",
       ".output_html .nn { color: #0000FF; font-weight: bold } /* Name.Namespace */\n",
       ".output_html .nt { color: #008000; font-weight: bold } /* Name.Tag */\n",
       ".output_html .nv { color: #19177C } /* Name.Variable */\n",
       ".output_html .ow { color: #AA22FF; font-weight: bold } /* Operator.Word */\n",
       ".output_html .w { color: #bbbbbb } /* Text.Whitespace */\n",
       ".output_html .mb { color: #666666 } /* Literal.Number.Bin */\n",
       ".output_html .mf { color: #666666 } /* Literal.Number.Float */\n",
       ".output_html .mh { color: #666666 } /* Literal.Number.Hex */\n",
       ".output_html .mi { color: #666666 } /* Literal.Number.Integer */\n",
       ".output_html .mo { color: #666666 } /* Literal.Number.Oct */\n",
       ".output_html .sa { color: #BA2121 } /* Literal.String.Affix */\n",
       ".output_html .sb { color: #BA2121 } /* Literal.String.Backtick */\n",
       ".output_html .sc { color: #BA2121 } /* Literal.String.Char */\n",
       ".output_html .dl { color: #BA2121 } /* Literal.String.Delimiter */\n",
       ".output_html .sd { color: #BA2121; font-style: italic } /* Literal.String.Doc */\n",
       ".output_html .s2 { color: #BA2121 } /* Literal.String.Double */\n",
       ".output_html .se { color: #AA5D1F; font-weight: bold } /* Literal.String.Escape */\n",
       ".output_html .sh { color: #BA2121 } /* Literal.String.Heredoc */\n",
       ".output_html .si { color: #A45A77; font-weight: bold } /* Literal.String.Interpol */\n",
       ".output_html .sx { color: #008000 } /* Literal.String.Other */\n",
       ".output_html .sr { color: #A45A77 } /* Literal.String.Regex */\n",
       ".output_html .s1 { color: #BA2121 } /* Literal.String.Single */\n",
       ".output_html .ss { color: #19177C } /* Literal.String.Symbol */\n",
       ".output_html .bp { color: #008000 } /* Name.Builtin.Pseudo */\n",
       ".output_html .fm { color: #0000FF } /* Name.Function.Magic */\n",
       ".output_html .vc { color: #19177C } /* Name.Variable.Class */\n",
       ".output_html .vg { color: #19177C } /* Name.Variable.Global */\n",
       ".output_html .vi { color: #19177C } /* Name.Variable.Instance */\n",
       ".output_html .vm { color: #19177C } /* Name.Variable.Magic */\n",
       ".output_html .il { color: #666666 } /* Literal.Number.Integer.Long */</style><div class=\"highlight\"><pre><span></span><span class=\"p\">{</span>\n",
       "<span class=\"w\">    </span><span class=\"nt\">&quot;viggo-train&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"p\">{</span>\n",
       "<span class=\"w\">        </span><span class=\"nt\">&quot;file_name&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"s2\">&quot;/mnt/cluster_storage/viggo/train.jsonl&quot;</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">        </span><span class=\"nt\">&quot;formatting&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"s2\">&quot;alpaca&quot;</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">        </span><span class=\"nt\">&quot;columns&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"p\">{</span>\n",
       "<span class=\"w\">            </span><span class=\"nt\">&quot;prompt&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"s2\">&quot;instruction&quot;</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">            </span><span class=\"nt\">&quot;query&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"s2\">&quot;input&quot;</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">            </span><span class=\"nt\">&quot;response&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"s2\">&quot;output&quot;</span>\n",
       "<span class=\"w\">        </span><span class=\"p\">}</span>\n",
       "<span class=\"w\">    </span><span class=\"p\">},</span>\n",
       "<span class=\"w\">    </span><span class=\"nt\">&quot;viggo-val&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"p\">{</span>\n",
       "<span class=\"w\">        </span><span class=\"nt\">&quot;file_name&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"s2\">&quot;/mnt/cluster_storage/viggo/val.jsonl&quot;</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">        </span><span class=\"nt\">&quot;formatting&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"s2\">&quot;alpaca&quot;</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">        </span><span class=\"nt\">&quot;columns&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"p\">{</span>\n",
       "<span class=\"w\">            </span><span class=\"nt\">&quot;prompt&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"s2\">&quot;instruction&quot;</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">            </span><span class=\"nt\">&quot;query&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"s2\">&quot;input&quot;</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">            </span><span class=\"nt\">&quot;response&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"s2\">&quot;output&quot;</span>\n",
       "<span class=\"w\">        </span><span class=\"p\">}</span>\n",
       "<span class=\"w\">    </span><span class=\"p\">}</span>\n",
       "<span class=\"p\">}</span>\n",
       "</pre></div>\n"
      ],
      "text/latex": [
       "\\begin{Verbatim}[commandchars=\\\\\\{\\}]\n",
       "\\PY{p}{\\PYZob{}}\n",
       "\\PY{+w}{    }\\PY{n+nt}{\\PYZdq{}viggo\\PYZhy{}train\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{p}{\\PYZob{}}\n",
       "\\PY{+w}{        }\\PY{n+nt}{\\PYZdq{}file\\PYZus{}name\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+s+s2}{\\PYZdq{}/mnt/cluster\\PYZus{}storage/viggo/train.jsonl\\PYZdq{}}\\PY{p}{,}\n",
       "\\PY{+w}{        }\\PY{n+nt}{\\PYZdq{}formatting\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+s+s2}{\\PYZdq{}alpaca\\PYZdq{}}\\PY{p}{,}\n",
       "\\PY{+w}{        }\\PY{n+nt}{\\PYZdq{}columns\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{p}{\\PYZob{}}\n",
       "\\PY{+w}{            }\\PY{n+nt}{\\PYZdq{}prompt\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+s+s2}{\\PYZdq{}instruction\\PYZdq{}}\\PY{p}{,}\n",
       "\\PY{+w}{            }\\PY{n+nt}{\\PYZdq{}query\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+s+s2}{\\PYZdq{}input\\PYZdq{}}\\PY{p}{,}\n",
       "\\PY{+w}{            }\\PY{n+nt}{\\PYZdq{}response\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+s+s2}{\\PYZdq{}output\\PYZdq{}}\n",
       "\\PY{+w}{        }\\PY{p}{\\PYZcb{}}\n",
       "\\PY{+w}{    }\\PY{p}{\\PYZcb{},}\n",
       "\\PY{+w}{    }\\PY{n+nt}{\\PYZdq{}viggo\\PYZhy{}val\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{p}{\\PYZob{}}\n",
       "\\PY{+w}{        }\\PY{n+nt}{\\PYZdq{}file\\PYZus{}name\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+s+s2}{\\PYZdq{}/mnt/cluster\\PYZus{}storage/viggo/val.jsonl\\PYZdq{}}\\PY{p}{,}\n",
       "\\PY{+w}{        }\\PY{n+nt}{\\PYZdq{}formatting\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+s+s2}{\\PYZdq{}alpaca\\PYZdq{}}\\PY{p}{,}\n",
       "\\PY{+w}{        }\\PY{n+nt}{\\PYZdq{}columns\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{p}{\\PYZob{}}\n",
       "\\PY{+w}{            }\\PY{n+nt}{\\PYZdq{}prompt\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+s+s2}{\\PYZdq{}instruction\\PYZdq{}}\\PY{p}{,}\n",
       "\\PY{+w}{            }\\PY{n+nt}{\\PYZdq{}query\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+s+s2}{\\PYZdq{}input\\PYZdq{}}\\PY{p}{,}\n",
       "\\PY{+w}{            }\\PY{n+nt}{\\PYZdq{}response\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+s+s2}{\\PYZdq{}output\\PYZdq{}}\n",
       "\\PY{+w}{        }\\PY{p}{\\PYZcb{}}\n",
       "\\PY{+w}{    }\\PY{p}{\\PYZcb{}}\n",
       "\\PY{p}{\\PYZcb{}}\n",
       "\\end{Verbatim}\n"
      ],
      "text/plain": [
       "{\n",
       "    \"viggo-train\": {\n",
       "        \"file_name\": \"/mnt/cluster_storage/viggo/train.jsonl\",\n",
       "        \"formatting\": \"alpaca\",\n",
       "        \"columns\": {\n",
       "            \"prompt\": \"instruction\",\n",
       "            \"query\": \"input\",\n",
       "            \"response\": \"output\"\n",
       "        }\n",
       "    },\n",
       "    \"viggo-val\": {\n",
       "        \"file_name\": \"/mnt/cluster_storage/viggo/val.jsonl\",\n",
       "        \"formatting\": \"alpaca\",\n",
       "        \"columns\": {\n",
       "            \"prompt\": \"instruction\",\n",
       "            \"query\": \"input\",\n",
       "            \"response\": \"output\"\n",
       "        }\n",
       "    }\n",
       "}"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "display(Code(filename=\"/mnt/cluster_storage/viggo/dataset_info.json\", language=\"json\"))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Distributed fine-tuning"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Use [Ray Train](https://docs.ray.io/en/latest/train/train.html) + [LLaMA-Factory](https://github.com/hiyouga/LLaMA-Factory) to perform multi-node training. Find the parameters for the training workload, post-training method, dataset location, train/val details, etc. in the `llama3_lora_sft_ray.yaml` config file. See the recipes for even more post-training methods, like SFT, pretraining, PPO, DPO, KTO, etc. [on GitHub](https://github.com/hiyouga/LLaMA-Factory/tree/main/examples).\n",
    "\n",
    "**Note**: Ray also supports using other tools like [axolotl](https://axolotl-ai-cloud.github.io/axolotl/docs/ray-integration.html) or even [Ray Train + HF Accelerate + FSDP/DeepSpeed](https://docs.ray.io/en/latest/train/huggingface-accelerate.html) directly for complete control of your post-training workloads.\n",
    "\n",
    "<img src=\"https://raw.githubusercontent.com/anyscale/foundational-ray-app/refs/heads/main/images/distributed_training.png\" width=800>"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### `config`"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "import os\n",
    "from pathlib import Path\n",
    "import yaml"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<style>pre { line-height: 125%; }\n",
       "td.linenos .normal { color: inherit; background-color: transparent; padding-left: 5px; padding-right: 5px; }\n",
       "span.linenos { color: inherit; background-color: transparent; padding-left: 5px; padding-right: 5px; }\n",
       "td.linenos .special { color: #000000; background-color: #ffffc0; padding-left: 5px; padding-right: 5px; }\n",
       "span.linenos.special { color: #000000; background-color: #ffffc0; padding-left: 5px; padding-right: 5px; }\n",
       ".output_html .hll { background-color: #ffffcc }\n",
       ".output_html { background: #f8f8f8; }\n",
       ".output_html .c { color: #3D7B7B; font-style: italic } /* Comment */\n",
       ".output_html .err { border: 1px solid #FF0000 } /* Error */\n",
       ".output_html .k { color: #008000; font-weight: bold } /* Keyword */\n",
       ".output_html .o { color: #666666 } /* Operator */\n",
       ".output_html .ch { color: #3D7B7B; font-style: italic } /* Comment.Hashbang */\n",
       ".output_html .cm { color: #3D7B7B; font-style: italic } /* Comment.Multiline */\n",
       ".output_html .cp { color: #9C6500 } /* Comment.Preproc */\n",
       ".output_html .cpf { color: #3D7B7B; font-style: italic } /* Comment.PreprocFile */\n",
       ".output_html .c1 { color: #3D7B7B; font-style: italic } /* Comment.Single */\n",
       ".output_html .cs { color: #3D7B7B; font-style: italic } /* Comment.Special */\n",
       ".output_html .gd { color: #A00000 } /* Generic.Deleted */\n",
       ".output_html .ge { font-style: italic } /* Generic.Emph */\n",
       ".output_html .ges { font-weight: bold; font-style: italic } /* Generic.EmphStrong */\n",
       ".output_html .gr { color: #E40000 } /* Generic.Error */\n",
       ".output_html .gh { color: #000080; font-weight: bold } /* Generic.Heading */\n",
       ".output_html .gi { color: #008400 } /* Generic.Inserted */\n",
       ".output_html .go { color: #717171 } /* Generic.Output */\n",
       ".output_html .gp { color: #000080; font-weight: bold } /* Generic.Prompt */\n",
       ".output_html .gs { font-weight: bold } /* Generic.Strong */\n",
       ".output_html .gu { color: #800080; font-weight: bold } /* Generic.Subheading */\n",
       ".output_html .gt { color: #0044DD } /* Generic.Traceback */\n",
       ".output_html .kc { color: #008000; font-weight: bold } /* Keyword.Constant */\n",
       ".output_html .kd { color: #008000; font-weight: bold } /* Keyword.Declaration */\n",
       ".output_html .kn { color: #008000; font-weight: bold } /* Keyword.Namespace */\n",
       ".output_html .kp { color: #008000 } /* Keyword.Pseudo */\n",
       ".output_html .kr { color: #008000; font-weight: bold } /* Keyword.Reserved */\n",
       ".output_html .kt { color: #B00040 } /* Keyword.Type */\n",
       ".output_html .m { color: #666666 } /* Literal.Number */\n",
       ".output_html .s { color: #BA2121 } /* Literal.String */\n",
       ".output_html .na { color: #687822 } /* Name.Attribute */\n",
       ".output_html .nb { color: #008000 } /* Name.Builtin */\n",
       ".output_html .nc { color: #0000FF; font-weight: bold } /* Name.Class */\n",
       ".output_html .no { color: #880000 } /* Name.Constant */\n",
       ".output_html .nd { color: #AA22FF } /* Name.Decorator */\n",
       ".output_html .ni { color: #717171; font-weight: bold } /* Name.Entity */\n",
       ".output_html .ne { color: #CB3F38; font-weight: bold } /* Name.Exception */\n",
       ".output_html .nf { color: #0000FF } /* Name.Function */\n",
       ".output_html .nl { color: #767600 } /* Name.Label */\n",
       ".output_html .nn { color: #0000FF; font-weight: bold } /* Name.Namespace */\n",
       ".output_html .nt { color: #008000; font-weight: bold } /* Name.Tag */\n",
       ".output_html .nv { color: #19177C } /* Name.Variable */\n",
       ".output_html .ow { color: #AA22FF; font-weight: bold } /* Operator.Word */\n",
       ".output_html .w { color: #bbbbbb } /* Text.Whitespace */\n",
       ".output_html .mb { color: #666666 } /* Literal.Number.Bin */\n",
       ".output_html .mf { color: #666666 } /* Literal.Number.Float */\n",
       ".output_html .mh { color: #666666 } /* Literal.Number.Hex */\n",
       ".output_html .mi { color: #666666 } /* Literal.Number.Integer */\n",
       ".output_html .mo { color: #666666 } /* Literal.Number.Oct */\n",
       ".output_html .sa { color: #BA2121 } /* Literal.String.Affix */\n",
       ".output_html .sb { color: #BA2121 } /* Literal.String.Backtick */\n",
       ".output_html .sc { color: #BA2121 } /* Literal.String.Char */\n",
       ".output_html .dl { color: #BA2121 } /* Literal.String.Delimiter */\n",
       ".output_html .sd { color: #BA2121; font-style: italic } /* Literal.String.Doc */\n",
       ".output_html .s2 { color: #BA2121 } /* Literal.String.Double */\n",
       ".output_html .se { color: #AA5D1F; font-weight: bold } /* Literal.String.Escape */\n",
       ".output_html .sh { color: #BA2121 } /* Literal.String.Heredoc */\n",
       ".output_html .si { color: #A45A77; font-weight: bold } /* Literal.String.Interpol */\n",
       ".output_html .sx { color: #008000 } /* Literal.String.Other */\n",
       ".output_html .sr { color: #A45A77 } /* Literal.String.Regex */\n",
       ".output_html .s1 { color: #BA2121 } /* Literal.String.Single */\n",
       ".output_html .ss { color: #19177C } /* Literal.String.Symbol */\n",
       ".output_html .bp { color: #008000 } /* Name.Builtin.Pseudo */\n",
       ".output_html .fm { color: #0000FF } /* Name.Function.Magic */\n",
       ".output_html .vc { color: #19177C } /* Name.Variable.Class */\n",
       ".output_html .vg { color: #19177C } /* Name.Variable.Global */\n",
       ".output_html .vi { color: #19177C } /* Name.Variable.Instance */\n",
       ".output_html .vm { color: #19177C } /* Name.Variable.Magic */\n",
       ".output_html .il { color: #666666 } /* Literal.Number.Integer.Long */</style><div class=\"highlight\"><pre><span></span><span class=\"c1\">### model</span>\n",
       "<span class=\"nt\">model_name_or_path</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">Qwen/Qwen2.5-7B-Instruct</span>\n",
       "<span class=\"nt\">trust_remote_code</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">true</span>\n",
       "\n",
       "<span class=\"c1\">### method</span>\n",
       "<span class=\"nt\">stage</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">sft</span>\n",
       "<span class=\"nt\">do_train</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">true</span>\n",
       "<span class=\"nt\">finetuning_type</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">lora</span>\n",
       "<span class=\"nt\">lora_rank</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">8</span>\n",
       "<span class=\"nt\">lora_target</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">all</span>\n",
       "\n",
       "<span class=\"c1\">### dataset</span>\n",
       "<span class=\"nt\">dataset</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">viggo-train</span>\n",
       "<span class=\"nt\">dataset_dir</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">/mnt/cluster_storage/viggo</span><span class=\"w\">  </span><span class=\"c1\"># shared storage workers have access to</span>\n",
       "<span class=\"nt\">template</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">qwen</span>\n",
       "<span class=\"nt\">cutoff_len</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">2048</span>\n",
       "<span class=\"nt\">max_samples</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">1000</span>\n",
       "<span class=\"nt\">overwrite_cache</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">true</span>\n",
       "<span class=\"nt\">preprocessing_num_workers</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">16</span>\n",
       "<span class=\"nt\">dataloader_num_workers</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">4</span>\n",
       "\n",
       "<span class=\"c1\">### output</span>\n",
       "<span class=\"nt\">output_dir</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">/mnt/cluster_storage/viggo/outputs</span><span class=\"w\">  </span><span class=\"c1\"># should be somewhere workers have access to (ex. s3, nfs)</span>\n",
       "<span class=\"nt\">logging_steps</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">10</span>\n",
       "<span class=\"nt\">save_steps</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">500</span>\n",
       "<span class=\"nt\">plot_loss</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">true</span>\n",
       "<span class=\"nt\">overwrite_output_dir</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">true</span>\n",
       "<span class=\"nt\">save_only_model</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">false</span>\n",
       "\n",
       "<span class=\"c1\">### ray</span>\n",
       "<span class=\"nt\">ray_run_name</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">lora_sft_ray</span>\n",
       "<span class=\"nt\">ray_storage_path</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">/mnt/cluster_storage/viggo/saves</span><span class=\"w\">  </span><span class=\"c1\"># should be somewhere workers have access to (ex. s3, nfs)</span>\n",
       "<span class=\"nt\">ray_num_workers</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">4</span>\n",
       "<span class=\"nt\">resources_per_worker</span><span class=\"p\">:</span>\n",
       "<span class=\"w\">  </span><span class=\"nt\">GPU</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">1</span>\n",
       "<span class=\"w\">  </span><span class=\"nt\">anyscale/accelerator_shape:4xL4</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">0.001</span><span class=\"w\">  </span><span class=\"c1\"># Use this to specify a specific node shape,</span>\n",
       "<span class=\"w\">  </span><span class=\"c1\"># accelerator_type:L4: 1           # Or use this to simply specify a GPU type.</span>\n",
       "<span class=\"w\">  </span><span class=\"c1\"># see https://docs.ray.io/en/master/ray-core/accelerator-types.html#accelerator-types for a full list of accelerator types</span>\n",
       "<span class=\"nt\">placement_strategy</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">PACK</span>\n",
       "\n",
       "<span class=\"c1\">### train</span>\n",
       "<span class=\"nt\">per_device_train_batch_size</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">1</span>\n",
       "<span class=\"nt\">gradient_accumulation_steps</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">8</span>\n",
       "<span class=\"nt\">learning_rate</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">1.0e-4</span>\n",
       "<span class=\"nt\">num_train_epochs</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">5.0</span>\n",
       "<span class=\"nt\">lr_scheduler_type</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">cosine</span>\n",
       "<span class=\"nt\">warmup_ratio</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">0.1</span>\n",
       "<span class=\"nt\">bf16</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">true</span>\n",
       "<span class=\"nt\">ddp_timeout</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">180000000</span>\n",
       "<span class=\"nt\">resume_from_checkpoint</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">null</span>\n",
       "\n",
       "<span class=\"c1\">### eval</span>\n",
       "<span class=\"nt\">eval_dataset</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">viggo-val</span><span class=\"w\">  </span><span class=\"c1\"># uses same dataset_dir as training data</span>\n",
       "<span class=\"c1\"># val_size: 0.1  # only if using part of training data for validation</span>\n",
       "<span class=\"nt\">per_device_eval_batch_size</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">1</span>\n",
       "<span class=\"nt\">eval_strategy</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">steps</span>\n",
       "<span class=\"nt\">eval_steps</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"l l-Scalar l-Scalar-Plain\">500</span>\n",
       "</pre></div>\n"
      ],
      "text/latex": [
       "\\begin{Verbatim}[commandchars=\\\\\\{\\}]\n",
       "\\PY{c+c1}{\\PYZsh{}\\PYZsh{}\\PYZsh{} model}\n",
       "\\PY{n+nt}{model\\PYZus{}name\\PYZus{}or\\PYZus{}path}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{Qwen/Qwen2.5\\PYZhy{}7B\\PYZhy{}Instruct}\n",
       "\\PY{n+nt}{trust\\PYZus{}remote\\PYZus{}code}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{true}\n",
       "\n",
       "\\PY{c+c1}{\\PYZsh{}\\PYZsh{}\\PYZsh{} method}\n",
       "\\PY{n+nt}{stage}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{sft}\n",
       "\\PY{n+nt}{do\\PYZus{}train}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{true}\n",
       "\\PY{n+nt}{finetuning\\PYZus{}type}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{lora}\n",
       "\\PY{n+nt}{lora\\PYZus{}rank}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{8}\n",
       "\\PY{n+nt}{lora\\PYZus{}target}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{all}\n",
       "\n",
       "\\PY{c+c1}{\\PYZsh{}\\PYZsh{}\\PYZsh{} dataset}\n",
       "\\PY{n+nt}{dataset}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{viggo\\PYZhy{}train}\n",
       "\\PY{n+nt}{dataset\\PYZus{}dir}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{/mnt/cluster\\PYZus{}storage/viggo}\\PY{+w}{  }\\PY{c+c1}{\\PYZsh{} shared storage workers have access to}\n",
       "\\PY{n+nt}{template}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{qwen}\n",
       "\\PY{n+nt}{cutoff\\PYZus{}len}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{2048}\n",
       "\\PY{n+nt}{max\\PYZus{}samples}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{1000}\n",
       "\\PY{n+nt}{overwrite\\PYZus{}cache}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{true}\n",
       "\\PY{n+nt}{preprocessing\\PYZus{}num\\PYZus{}workers}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{16}\n",
       "\\PY{n+nt}{dataloader\\PYZus{}num\\PYZus{}workers}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{4}\n",
       "\n",
       "\\PY{c+c1}{\\PYZsh{}\\PYZsh{}\\PYZsh{} output}\n",
       "\\PY{n+nt}{output\\PYZus{}dir}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{/mnt/cluster\\PYZus{}storage/viggo/outputs}\\PY{+w}{  }\\PY{c+c1}{\\PYZsh{} should be somewhere workers have access to (ex. s3, nfs)}\n",
       "\\PY{n+nt}{logging\\PYZus{}steps}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{10}\n",
       "\\PY{n+nt}{save\\PYZus{}steps}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{500}\n",
       "\\PY{n+nt}{plot\\PYZus{}loss}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{true}\n",
       "\\PY{n+nt}{overwrite\\PYZus{}output\\PYZus{}dir}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{true}\n",
       "\\PY{n+nt}{save\\PYZus{}only\\PYZus{}model}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{false}\n",
       "\n",
       "\\PY{c+c1}{\\PYZsh{}\\PYZsh{}\\PYZsh{} ray}\n",
       "\\PY{n+nt}{ray\\PYZus{}run\\PYZus{}name}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{lora\\PYZus{}sft\\PYZus{}ray}\n",
       "\\PY{n+nt}{ray\\PYZus{}storage\\PYZus{}path}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{/mnt/cluster\\PYZus{}storage/viggo/saves}\\PY{+w}{  }\\PY{c+c1}{\\PYZsh{} should be somewhere workers have access to (ex. s3, nfs)}\n",
       "\\PY{n+nt}{ray\\PYZus{}num\\PYZus{}workers}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{4}\n",
       "\\PY{n+nt}{resources\\PYZus{}per\\PYZus{}worker}\\PY{p}{:}\n",
       "\\PY{+w}{  }\\PY{n+nt}{GPU}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{1}\n",
       "\\PY{+w}{  }\\PY{n+nt}{anyscale/accelerator\\PYZus{}shape:4xL4}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{0.001}\\PY{+w}{  }\\PY{c+c1}{\\PYZsh{} Use this to specify a specific node shape,}\n",
       "\\PY{+w}{  }\\PY{c+c1}{\\PYZsh{} accelerator\\PYZus{}type:L4: 1           \\PYZsh{} Or use this to simply specify a GPU type.}\n",
       "\\PY{+w}{  }\\PY{c+c1}{\\PYZsh{} see https://docs.ray.io/en/master/ray\\PYZhy{}core/accelerator\\PYZhy{}types.html\\PYZsh{}accelerator\\PYZhy{}types for a full list of accelerator types}\n",
       "\\PY{n+nt}{placement\\PYZus{}strategy}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{PACK}\n",
       "\n",
       "\\PY{c+c1}{\\PYZsh{}\\PYZsh{}\\PYZsh{} train}\n",
       "\\PY{n+nt}{per\\PYZus{}device\\PYZus{}train\\PYZus{}batch\\PYZus{}size}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{1}\n",
       "\\PY{n+nt}{gradient\\PYZus{}accumulation\\PYZus{}steps}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{8}\n",
       "\\PY{n+nt}{learning\\PYZus{}rate}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{1.0e\\PYZhy{}4}\n",
       "\\PY{n+nt}{num\\PYZus{}train\\PYZus{}epochs}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{5.0}\n",
       "\\PY{n+nt}{lr\\PYZus{}scheduler\\PYZus{}type}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{cosine}\n",
       "\\PY{n+nt}{warmup\\PYZus{}ratio}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{0.1}\n",
       "\\PY{n+nt}{bf16}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{true}\n",
       "\\PY{n+nt}{ddp\\PYZus{}timeout}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{180000000}\n",
       "\\PY{n+nt}{resume\\PYZus{}from\\PYZus{}checkpoint}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{null}\n",
       "\n",
       "\\PY{c+c1}{\\PYZsh{}\\PYZsh{}\\PYZsh{} eval}\n",
       "\\PY{n+nt}{eval\\PYZus{}dataset}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{viggo\\PYZhy{}val}\\PY{+w}{  }\\PY{c+c1}{\\PYZsh{} uses same dataset\\PYZus{}dir as training data}\n",
       "\\PY{c+c1}{\\PYZsh{} val\\PYZus{}size: 0.1  \\PYZsh{} only if using part of training data for validation}\n",
       "\\PY{n+nt}{per\\PYZus{}device\\PYZus{}eval\\PYZus{}batch\\PYZus{}size}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{1}\n",
       "\\PY{n+nt}{eval\\PYZus{}strategy}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{steps}\n",
       "\\PY{n+nt}{eval\\PYZus{}steps}\\PY{p}{:}\\PY{+w}{ }\\PY{l+lScalar+lScalarPlain}{500}\n",
       "\\end{Verbatim}\n"
      ],
      "text/plain": [
       "### model\n",
       "model_name_or_path: Qwen/Qwen2.5-7B-Instruct\n",
       "trust_remote_code: true\n",
       "\n",
       "### method\n",
       "stage: sft\n",
       "do_train: true\n",
       "finetuning_type: lora\n",
       "lora_rank: 8\n",
       "lora_target: all\n",
       "\n",
       "### dataset\n",
       "dataset: viggo-train\n",
       "dataset_dir: /mnt/cluster_storage/viggo  # shared storage workers have access to\n",
       "template: qwen\n",
       "cutoff_len: 2048\n",
       "max_samples: 1000\n",
       "overwrite_cache: true\n",
       "preprocessing_num_workers: 16\n",
       "dataloader_num_workers: 4\n",
       "\n",
       "### output\n",
       "output_dir: /mnt/cluster_storage/viggo/outputs  # should be somewhere workers have access to (ex. s3, nfs)\n",
       "logging_steps: 10\n",
       "save_steps: 500\n",
       "plot_loss: true\n",
       "overwrite_output_dir: true\n",
       "save_only_model: false\n",
       "\n",
       "### ray\n",
       "ray_run_name: lora_sft_ray\n",
       "ray_storage_path: /mnt/cluster_storage/viggo/saves  # should be somewhere workers have access to (ex. s3, nfs)\n",
       "ray_num_workers: 4\n",
       "resources_per_worker:\n",
       "  GPU: 1\n",
       "  anyscale/accelerator_shape:4xL4: 0.001  # Use this to specify a specific node shape,\n",
       "  # accelerator_type:L4: 1           # Or use this to simply specify a GPU type.\n",
       "  # see https://docs.ray.io/en/master/ray-core/accelerator-types.html#accelerator-types for a full list of accelerator types\n",
       "placement_strategy: PACK\n",
       "\n",
       "### train\n",
       "per_device_train_batch_size: 1\n",
       "gradient_accumulation_steps: 8\n",
       "learning_rate: 1.0e-4\n",
       "num_train_epochs: 5.0\n",
       "lr_scheduler_type: cosine\n",
       "warmup_ratio: 0.1\n",
       "bf16: true\n",
       "ddp_timeout: 180000000\n",
       "resume_from_checkpoint: null\n",
       "\n",
       "### eval\n",
       "eval_dataset: viggo-val  # uses same dataset_dir as training data\n",
       "# val_size: 0.1  # only if using part of training data for validation\n",
       "per_device_eval_batch_size: 1\n",
       "eval_strategy: steps\n",
       "eval_steps: 500"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "display(Code(filename=\"lora_sft_ray.yaml\", language=\"yaml\"))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Qwen/Qwen2.5-7B-Instruct\n"
     ]
    }
   ],
   "source": [
    "model_id = \"ft-model\"  # call it whatever you want\n",
    "model_source = yaml.safe_load(open(\"lora_sft_ray.yaml\"))[\"model_name_or_path\"]  # HF model ID, S3 mirror config, or GCS mirror config\n",
    "print (model_source)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### Multi-node training"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Use Ray Train + LlamaFactory to perform the mult-node train loop."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "<div class=\"alert alert-block alert\"> <b>Ray Train</b> \n",
    "\n",
    "Using [Ray Train](https://docs.ray.io/en/latest/train/train.html) has several advantages:\n",
    "- it automatically handles **multi-node, multi-GPU** setup with no manual SSH setup or `hostfile` configs. \n",
    "- you can define **per-worker fractional resource requirements**, for example, 2 CPUs and 0.5 GPU per worker.\n",
    "- you can run on **heterogeneous machines** and scale flexibly, for example, CPU for preprocessing and GPU for training.\n",
    "- it has built-in **fault tolerance** through retry of failed workers, and continue from last checkpoint.\n",
    "- it supports Data Parallel, Model Parallel, Parameter Server, and even custom strategies.\n",
    "- [Ray Compiled graphs](https://docs.ray.io/en/latest/ray-core/compiled-graph/ray-compiled-graph.html) allow you to even define different parallelism for jointly optimizing multiple models. Megatron, DeepSpeed, and similar frameworks only allow for one global setting.\n",
    "\n",
    "[RayTurbo Train](https://docs.anyscale.com/rayturbo/rayturbo-train) offers even more improvement to the price-performance ratio, performance monitoring, and more:\n",
    "- **elastic training** to scale to a dynamic number of workers, and continue training on fewer resources, even on spot instances.\n",
    "- **purpose-built dashboard** designed to streamline the debugging of Ray Train workloads:\n",
    "    - Monitoring: View the status of training runs and train workers.\n",
    "    - Metrics: See insights on training throughput and training system operation time.\n",
    "    - Profiling: Investigate bottlenecks, hangs, or errors from individual training worker processes.\n",
    "\n",
    "<img src=\"https://raw.githubusercontent.com/anyscale/foundational-ray-app/refs/heads/main/images/train_dashboard.png\" width=700>"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "\n",
      "Training started with configuration:\n",
      "    ╭──────────────────────────────────────────────────────────────────────────────────────────────────────╮\n",
      "    │ Training config                                                                                      │\n",
      "    ├──────────────────────────────────────────────────────────────────────────────────────────────────────┤\n",
      "    │ train_loop_config/args/bf16                                                                     True │\n",
      "    │ train_loop_config/args/cutoff_len                                                               2048 │\n",
      "    │ train_loop_config/args/dataloader_num_workers                                                      4 │\n",
      "    │ train_loop_config/args/dataset                                                           viggo-train │\n",
      "    │ train_loop_config/args/dataset_dir                                              ...ter_storage/viggo │\n",
      "    │ train_loop_config/args/ddp_timeout                                                         180000000 │\n",
      "    │ train_loop_config/args/do_train                                                                 True │\n",
      "    │ train_loop_config/args/eval_dataset                                                        viggo-val │\n",
      "    │ train_loop_config/args/eval_steps                                                                500 │\n",
      "    │ train_loop_config/args/eval_strategy                                                           steps │\n",
      "    │ train_loop_config/args/finetuning_type                                                          lora │\n",
      "    │ train_loop_config/args/gradient_accumulation_steps                                                 8 │\n",
      "    │ train_loop_config/args/learning_rate                                                          0.0001 │\n",
      "    │ train_loop_config/args/logging_steps                                                              10 │\n",
      "    │ train_loop_config/args/lora_rank                                                                   8 │\n",
      "    │ train_loop_config/args/lora_target                                                               all │\n",
      "    │ train_loop_config/args/lr_scheduler_type                                                      cosine │\n",
      "    │ train_loop_config/args/max_samples                                                              1000 │\n",
      "    │ train_loop_config/args/model_name_or_path                                       ...en2.5-7B-Instruct │\n",
      "    │ train_loop_config/args/num_train_epochs                                                          5.0 │\n",
      "    │ train_loop_config/args/output_dir                                               ...age/viggo/outputs │\n",
      "    │ train_loop_config/args/overwrite_cache                                                          True │\n",
      "    │ train_loop_config/args/overwrite_output_dir                                                     True │\n",
      "    │ train_loop_config/args/per_device_eval_batch_size                                                  1 │\n",
      "    │ train_loop_config/args/per_device_train_batch_size                                                 1 │\n",
      "    │ train_loop_config/args/placement_strategy                                                       PACK │\n",
      "    │ train_loop_config/args/plot_loss                                                                True │\n",
      "    │ train_loop_config/args/preprocessing_num_workers                                                  16 │\n",
      "    │ train_loop_config/args/ray_num_workers                                                             4 │\n",
      "    │ train_loop_config/args/ray_run_name                                                     lora_sft_ray │\n",
      "    │ train_loop_config/args/ray_storage_path                                         ...orage/viggo/saves │\n",
      "    │ train_loop_config/args/resources_per_worker/GPU                                                    1 │\n",
      "    │ train_loop_config/args/resources_per_worker/anyscale/accelerator_shape:4xA10G                      1 │\n",
      "    │ train_loop_config/args/resume_from_checkpoint                                                        │\n",
      "    │ train_loop_config/args/save_only_model                                                         False │\n",
      "    │ train_loop_config/args/save_steps                                                                500 │\n",
      "    │ train_loop_config/args/stage                                                                     sft │\n",
      "    │ train_loop_config/args/template                                                                 qwen │\n",
      "    │ train_loop_config/args/trust_remote_code                                                        True │\n",
      "    │ train_loop_config/args/warmup_ratio                                                              0.1 │\n",
      "    │ train_loop_config/callbacks                                                     ... 0x7e1262910e10>] │\n",
      "    ╰──────────────────────────────────────────────────────────────────────────────────────────────────────╯\n",
      "\n",
      "    100%|██████████| 155/155 [07:12<00:00,  2.85s/it][INFO|trainer.py:3942] 2025-04-11 14:57:59,207 >> Saving model checkpoint to /mnt/cluster_storage/viggo/outputs/checkpoint-155\n",
      "    \n",
      "    Training finished iteration 1 at 2025-04-11 14:58:02. Total running time: 10min 24s\n",
      "    ╭─────────────────────────────────────────╮\n",
      "    │ Training result                         │\n",
      "    ├─────────────────────────────────────────┤\n",
      "    │ checkpoint_dir_name   checkpoint_000000 │\n",
      "    │ time_this_iter_s              521.83827 │\n",
      "    │ time_total_s                  521.83827 │\n",
      "    │ training_iteration                    1 │\n",
      "    │ epoch                             4.704 │\n",
      "    │ grad_norm                       0.14288 │\n",
      "    │ learning_rate                        0. │\n",
      "    │ loss                             0.0065 │\n",
      "    │ step                                150 │\n",
      "    ╰─────────────────────────────────────────╯\n",
      "    Training saved a checkpoint for iteration 1 at: (local)/mnt/cluster_storage/viggo/saves/lora_sft_ray/TorchTrainer_95d16_00000_0_2025-04-11_14-47-37/checkpoint_000000\n",
      "\n",
      "\n",
      "\n"
     ]
    }
   ],
   "source": [
    "%%bash\n",
    "# Run multi-node distributed fine-tuning workload\n",
    "USE_RAY=1 llamafactory-cli train lora_sft_ray.yaml"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<style>pre { line-height: 125%; }\n",
       "td.linenos .normal { color: inherit; background-color: transparent; padding-left: 5px; padding-right: 5px; }\n",
       "span.linenos { color: inherit; background-color: transparent; padding-left: 5px; padding-right: 5px; }\n",
       "td.linenos .special { color: #000000; background-color: #ffffc0; padding-left: 5px; padding-right: 5px; }\n",
       "span.linenos.special { color: #000000; background-color: #ffffc0; padding-left: 5px; padding-right: 5px; }\n",
       ".output_html .hll { background-color: #ffffcc }\n",
       ".output_html { background: #f8f8f8; }\n",
       ".output_html .c { color: #3D7B7B; font-style: italic } /* Comment */\n",
       ".output_html .err { border: 1px solid #FF0000 } /* Error */\n",
       ".output_html .k { color: #008000; font-weight: bold } /* Keyword */\n",
       ".output_html .o { color: #666666 } /* Operator */\n",
       ".output_html .ch { color: #3D7B7B; font-style: italic } /* Comment.Hashbang */\n",
       ".output_html .cm { color: #3D7B7B; font-style: italic } /* Comment.Multiline */\n",
       ".output_html .cp { color: #9C6500 } /* Comment.Preproc */\n",
       ".output_html .cpf { color: #3D7B7B; font-style: italic } /* Comment.PreprocFile */\n",
       ".output_html .c1 { color: #3D7B7B; font-style: italic } /* Comment.Single */\n",
       ".output_html .cs { color: #3D7B7B; font-style: italic } /* Comment.Special */\n",
       ".output_html .gd { color: #A00000 } /* Generic.Deleted */\n",
       ".output_html .ge { font-style: italic } /* Generic.Emph */\n",
       ".output_html .ges { font-weight: bold; font-style: italic } /* Generic.EmphStrong */\n",
       ".output_html .gr { color: #E40000 } /* Generic.Error */\n",
       ".output_html .gh { color: #000080; font-weight: bold } /* Generic.Heading */\n",
       ".output_html .gi { color: #008400 } /* Generic.Inserted */\n",
       ".output_html .go { color: #717171 } /* Generic.Output */\n",
       ".output_html .gp { color: #000080; font-weight: bold } /* Generic.Prompt */\n",
       ".output_html .gs { font-weight: bold } /* Generic.Strong */\n",
       ".output_html .gu { color: #800080; font-weight: bold } /* Generic.Subheading */\n",
       ".output_html .gt { color: #0044DD } /* Generic.Traceback */\n",
       ".output_html .kc { color: #008000; font-weight: bold } /* Keyword.Constant */\n",
       ".output_html .kd { color: #008000; font-weight: bold } /* Keyword.Declaration */\n",
       ".output_html .kn { color: #008000; font-weight: bold } /* Keyword.Namespace */\n",
       ".output_html .kp { color: #008000 } /* Keyword.Pseudo */\n",
       ".output_html .kr { color: #008000; font-weight: bold } /* Keyword.Reserved */\n",
       ".output_html .kt { color: #B00040 } /* Keyword.Type */\n",
       ".output_html .m { color: #666666 } /* Literal.Number */\n",
       ".output_html .s { color: #BA2121 } /* Literal.String */\n",
       ".output_html .na { color: #687822 } /* Name.Attribute */\n",
       ".output_html .nb { color: #008000 } /* Name.Builtin */\n",
       ".output_html .nc { color: #0000FF; font-weight: bold } /* Name.Class */\n",
       ".output_html .no { color: #880000 } /* Name.Constant */\n",
       ".output_html .nd { color: #AA22FF } /* Name.Decorator */\n",
       ".output_html .ni { color: #717171; font-weight: bold } /* Name.Entity */\n",
       ".output_html .ne { color: #CB3F38; font-weight: bold } /* Name.Exception */\n",
       ".output_html .nf { color: #0000FF } /* Name.Function */\n",
       ".output_html .nl { color: #767600 } /* Name.Label */\n",
       ".output_html .nn { color: #0000FF; font-weight: bold } /* Name.Namespace */\n",
       ".output_html .nt { color: #008000; font-weight: bold } /* Name.Tag */\n",
       ".output_html .nv { color: #19177C } /* Name.Variable */\n",
       ".output_html .ow { color: #AA22FF; font-weight: bold } /* Operator.Word */\n",
       ".output_html .w { color: #bbbbbb } /* Text.Whitespace */\n",
       ".output_html .mb { color: #666666 } /* Literal.Number.Bin */\n",
       ".output_html .mf { color: #666666 } /* Literal.Number.Float */\n",
       ".output_html .mh { color: #666666 } /* Literal.Number.Hex */\n",
       ".output_html .mi { color: #666666 } /* Literal.Number.Integer */\n",
       ".output_html .mo { color: #666666 } /* Literal.Number.Oct */\n",
       ".output_html .sa { color: #BA2121 } /* Literal.String.Affix */\n",
       ".output_html .sb { color: #BA2121 } /* Literal.String.Backtick */\n",
       ".output_html .sc { color: #BA2121 } /* Literal.String.Char */\n",
       ".output_html .dl { color: #BA2121 } /* Literal.String.Delimiter */\n",
       ".output_html .sd { color: #BA2121; font-style: italic } /* Literal.String.Doc */\n",
       ".output_html .s2 { color: #BA2121 } /* Literal.String.Double */\n",
       ".output_html .se { color: #AA5D1F; font-weight: bold } /* Literal.String.Escape */\n",
       ".output_html .sh { color: #BA2121 } /* Literal.String.Heredoc */\n",
       ".output_html .si { color: #A45A77; font-weight: bold } /* Literal.String.Interpol */\n",
       ".output_html .sx { color: #008000 } /* Literal.String.Other */\n",
       ".output_html .sr { color: #A45A77 } /* Literal.String.Regex */\n",
       ".output_html .s1 { color: #BA2121 } /* Literal.String.Single */\n",
       ".output_html .ss { color: #19177C } /* Literal.String.Symbol */\n",
       ".output_html .bp { color: #008000 } /* Name.Builtin.Pseudo */\n",
       ".output_html .fm { color: #0000FF } /* Name.Function.Magic */\n",
       ".output_html .vc { color: #19177C } /* Name.Variable.Class */\n",
       ".output_html .vg { color: #19177C } /* Name.Variable.Global */\n",
       ".output_html .vi { color: #19177C } /* Name.Variable.Instance */\n",
       ".output_html .vm { color: #19177C } /* Name.Variable.Magic */\n",
       ".output_html .il { color: #666666 } /* Literal.Number.Integer.Long */</style><div class=\"highlight\"><pre><span></span><span class=\"p\">{</span>\n",
       "<span class=\"w\">    </span><span class=\"nt\">&quot;epoch&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"mf\">4.864</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">    </span><span class=\"nt\">&quot;eval_viggo-val_loss&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"mf\">0.13618840277194977</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">    </span><span class=\"nt\">&quot;eval_viggo-val_runtime&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"mf\">20.2797</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">    </span><span class=\"nt\">&quot;eval_viggo-val_samples_per_second&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"mf\">35.208</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">    </span><span class=\"nt\">&quot;eval_viggo-val_steps_per_second&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"mf\">8.827</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">    </span><span class=\"nt\">&quot;total_flos&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"mf\">4.843098686147789e+16</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">    </span><span class=\"nt\">&quot;train_loss&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"mf\">0.2079355036479331</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">    </span><span class=\"nt\">&quot;train_runtime&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"mf\">437.2951</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">    </span><span class=\"nt\">&quot;train_samples_per_second&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"mf\">11.434</span><span class=\"p\">,</span>\n",
       "<span class=\"w\">    </span><span class=\"nt\">&quot;train_steps_per_second&quot;</span><span class=\"p\">:</span><span class=\"w\"> </span><span class=\"mf\">0.354</span>\n",
       "<span class=\"p\">}</span>\n",
       "</pre></div>\n"
      ],
      "text/latex": [
       "\\begin{Verbatim}[commandchars=\\\\\\{\\}]\n",
       "\\PY{p}{\\PYZob{}}\n",
       "\\PY{+w}{    }\\PY{n+nt}{\\PYZdq{}epoch\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+m+mf}{4.864}\\PY{p}{,}\n",
       "\\PY{+w}{    }\\PY{n+nt}{\\PYZdq{}eval\\PYZus{}viggo\\PYZhy{}val\\PYZus{}loss\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+m+mf}{0.13618840277194977}\\PY{p}{,}\n",
       "\\PY{+w}{    }\\PY{n+nt}{\\PYZdq{}eval\\PYZus{}viggo\\PYZhy{}val\\PYZus{}runtime\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+m+mf}{20.2797}\\PY{p}{,}\n",
       "\\PY{+w}{    }\\PY{n+nt}{\\PYZdq{}eval\\PYZus{}viggo\\PYZhy{}val\\PYZus{}samples\\PYZus{}per\\PYZus{}second\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+m+mf}{35.208}\\PY{p}{,}\n",
       "\\PY{+w}{    }\\PY{n+nt}{\\PYZdq{}eval\\PYZus{}viggo\\PYZhy{}val\\PYZus{}steps\\PYZus{}per\\PYZus{}second\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+m+mf}{8.827}\\PY{p}{,}\n",
       "\\PY{+w}{    }\\PY{n+nt}{\\PYZdq{}total\\PYZus{}flos\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+m+mf}{4.843098686147789e+16}\\PY{p}{,}\n",
       "\\PY{+w}{    }\\PY{n+nt}{\\PYZdq{}train\\PYZus{}loss\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+m+mf}{0.2079355036479331}\\PY{p}{,}\n",
       "\\PY{+w}{    }\\PY{n+nt}{\\PYZdq{}train\\PYZus{}runtime\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+m+mf}{437.2951}\\PY{p}{,}\n",
       "\\PY{+w}{    }\\PY{n+nt}{\\PYZdq{}train\\PYZus{}samples\\PYZus{}per\\PYZus{}second\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+m+mf}{11.434}\\PY{p}{,}\n",
       "\\PY{+w}{    }\\PY{n+nt}{\\PYZdq{}train\\PYZus{}steps\\PYZus{}per\\PYZus{}second\\PYZdq{}}\\PY{p}{:}\\PY{+w}{ }\\PY{l+m+mf}{0.354}\n",
       "\\PY{p}{\\PYZcb{}}\n",
       "\\end{Verbatim}\n"
      ],
      "text/plain": [
       "{\n",
       "    \"epoch\": 4.864,\n",
       "    \"eval_viggo-val_loss\": 0.13618840277194977,\n",
       "    \"eval_viggo-val_runtime\": 20.2797,\n",
       "    \"eval_viggo-val_samples_per_second\": 35.208,\n",
       "    \"eval_viggo-val_steps_per_second\": 8.827,\n",
       "    \"total_flos\": 4.843098686147789e+16,\n",
       "    \"train_loss\": 0.2079355036479331,\n",
       "    \"train_runtime\": 437.2951,\n",
       "    \"train_samples_per_second\": 11.434,\n",
       "    \"train_steps_per_second\": 0.354\n",
       "}"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "display(Code(filename=\"/mnt/cluster_storage/viggo/outputs/all_results.json\", language=\"json\"))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "vscode": {
     "languageId": "bat"
    }
   },
   "source": [
    "<img src=\"https://raw.githubusercontent.com/anyscale/e2e-llm-workflows/refs/heads/main/images/loss.png\" width=500>"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### Observability"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "<div class=\"alert alert-block alert\"> <b> 🔎 Monitoring and debugging with Ray</b> \n",
    "\n",
    "\n",
    "OSS Ray offers an extensive [observability suite](https://docs.ray.io/en/latest/ray-observability/index.html) with logs and an observability dashboard that you can use to monitor and debug. The dashboard includes a lot of different components such as:\n",
    "\n",
    "-  memory, utilization, etc., of the tasks running in the [cluster](https://docs.ray.io/en/latest/ray-observability/getting-started.html#dash-node-view)\n",
    "\n",
    "<img src=\"https://raw.githubusercontent.com/anyscale/foundational-ray-app/refs/heads/main/images/cluster_util.png\" width=700>\n",
    "\n",
    "- views to see all running tasks, utilization across instance types, autoscaling, etc.\n",
    "\n",
    "<img src=\"https://raw.githubusercontent.com/anyscale/foundational-ray-app/refs/heads/main/images/observability_views.png\" width=1000>\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "<div class=\"alert alert-block alert\"> <b> 🔎➕➕ Monitoring and debugging on Anyscale</b> \n",
    "\n",
    "OSS Ray comes with an extensive observability suite, and Anyscale takes it many steps further to make monitoring and debugging your workloads even easier and faster with:\n",
    "\n",
    "- [unified log viewer](https://docs.anyscale.com/monitoring/accessing-logs/) to see logs from *all* driver and worker processes\n",
    "- Ray workload specific dashboard, like Data, Train, etc., that can breakdown the tasks. For example, you can observe the preceding training workload live through the Train specific Ray Workloads dashboard:\n",
    "\n",
    "<img src=\"https://raw.githubusercontent.com/anyscale/e2e-llm-workflows/refs/heads/main/images/train_dashboard.png\" width=700>\n",
    "\n",
    "\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### Save to cloud storage"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "<div class=\"alert alert-block alert\"> <b> 🗂️ Storage on Anyscale</b> \n",
    "\n",
    "You can always store to data inside [any storage buckets](https://docs.anyscale.com/configuration/storage/#private-storage-buckets) but Anyscale offers a [default storage bucket](https://docs.anyscale.com/configuration/storage/#anyscale-default-storage-bucket) to make things even easier. You also have plenty of other [storage options](https://docs.anyscale.com/configuration/storage/) as well, shared at the cluster, user, and cloud levels."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "s3://anyscale-test-data-cld-i2w99rzq8b6lbjkke9y94vi5/org_7c1Kalm9WcX2bNIjW53GUT/cld_kvedZWag2qA8i5BjxUevf5i7/artifact_storage\n"
     ]
    }
   ],
   "source": [
    "%%bash\n",
    "# Anyscale default storage bucket.\n",
    "echo $ANYSCALE_ARTIFACT_STORAGE"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "%%bash\n",
    "# Save fine-tuning artifacts to cloud storage.\n",
    "STORAGE_PATH=\"$ANYSCALE_ARTIFACT_STORAGE/viggo\"\n",
    "LOCAL_OUTPUTS_PATH=\"/mnt/cluster_storage/viggo/outputs\"\n",
    "LOCAL_SAVES_PATH=\"/mnt/cluster_storage/viggo/saves\"\n",
    "\n",
    "# AWS S3 operations.\n",
    "if [[ \"$STORAGE_PATH\" == s3://* ]]; then\n",
    "    if aws s3 ls \"$STORAGE_PATH\" > /dev/null 2>&1; then\n",
    "        aws s3 rm \"$STORAGE_PATH\" --recursive --quiet\n",
    "    fi\n",
    "    aws s3 cp \"$LOCAL_OUTPUTS_PATH\" \"$STORAGE_PATH/outputs\" --recursive --quiet\n",
    "    aws s3 cp \"$LOCAL_SAVES_PATH\" \"$STORAGE_PATH/saves\" --recursive --quiet\n",
    "\n",
    "# Google Cloud Storage operations.\n",
    "elif [[ \"$STORAGE_PATH\" == gs://* ]]; then\n",
    "    if gsutil ls \"$STORAGE_PATH\" > /dev/null 2>&1; then\n",
    "        gsutil -m -q rm -r \"$STORAGE_PATH\"\n",
    "    fi\n",
    "    gsutil -m -q cp -r \"$LOCAL_OUTPUTS_PATH\" \"$STORAGE_PATH/outputs\"\n",
    "    gsutil -m -q cp -r \"$LOCAL_SAVES_PATH\" \"$STORAGE_PATH/saves\"\n",
    "\n",
    "else\n",
    "    echo \"Unsupported storage protocol: $STORAGE_PATH\"\n",
    "    exit 1\n",
    "fi"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "TorchTrainer_95d16_00000_0_2025-04-11_14-47-37\n",
      "TorchTrainer_f9e4e_00000_0_2025-04-11_12-41-34\n",
      "basic-variant-state-2025-04-11_12-41-34.json\n",
      "basic-variant-state-2025-04-11_14-47-37.json\n",
      "experiment_state-2025-04-11_12-41-34.json\n",
      "experiment_state-2025-04-11_14-47-37.json\n",
      "trainer.pkl\n",
      "tuner.pkl\n"
     ]
    }
   ],
   "source": [
    "%%bash\n",
    "ls /mnt/cluster_storage/viggo/saves/lora_sft_ray"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "/mnt/cluster_storage/viggo/saves/lora_sft_ray/TorchTrainer_95d16_00000_0_2025-04-11_14-47-37/checkpoint_000000/checkpoint\n",
      "s3://anyscale-test-data-cld-i2w99rzq8b6lbjkke9y94vi5/org_7c1Kalm9WcX2bNIjW53GUT/cld_kvedZWag2qA8i5BjxUevf5i7/artifact_storage/viggo/saves/lora_sft_ray/TorchTrainer_95d16_00000_0_2025-04-11_14-47-37/checkpoint_000000/checkpoint\n",
      "s3://anyscale-test-data-cld-i2w99rzq8b6lbjkke9y94vi5/org_7c1Kalm9WcX2bNIjW53GUT/cld_kvedZWag2qA8i5BjxUevf5i7/artifact_storage/viggo/saves/lora_sft_ray/TorchTrainer_95d16_00000_0_2025-04-11_14-47-37/checkpoint_000000\n",
      "checkpoint\n"
     ]
    }
   ],
   "source": [
    "# LoRA paths.\n",
    "save_dir = Path(\"/mnt/cluster_storage/viggo/saves/lora_sft_ray\")\n",
    "trainer_dirs = [d for d in save_dir.iterdir() if d.name.startswith(\"TorchTrainer_\") and d.is_dir()]\n",
    "latest_trainer = max(trainer_dirs, key=lambda d: d.stat().st_mtime, default=None)\n",
    "lora_path = f\"{latest_trainer}/checkpoint_000000/checkpoint\"\n",
    "cloud_lora_path = os.path.join(os.getenv(\"ANYSCALE_ARTIFACT_STORAGE\"), lora_path.split(\"/mnt/cluster_storage/\")[-1])\n",
    "dynamic_lora_path, lora_id = cloud_lora_path.rsplit(\"/\", 1)\n",
    "print (lora_path)\n",
    "print (cloud_lora_path)\n",
    "print (dynamic_lora_path)\n",
    "print (lora_id)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "README.md\n",
      "adapter_config.json\n",
      "adapter_model.safetensors\n",
      "added_tokens.json\n",
      "merges.txt\n",
      "optimizer.pt\n",
      "rng_state_0.pth\n",
      "rng_state_1.pth\n",
      "rng_state_2.pth\n",
      "rng_state_3.pth\n",
      "scheduler.pt\n",
      "special_tokens_map.json\n",
      "tokenizer.json\n",
      "tokenizer_config.json\n",
      "trainer_state.json\n",
      "training_args.bin\n",
      "vocab.json\n"
     ]
    }
   ],
   "source": [
    "%%bash -s \"$lora_path\"\n",
    "ls $1"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Batch inference \n",
    "[`Overview`](https://docs.ray.io/en/latest/data/working-with-llms.html) |  [`API reference`](https://docs.ray.io/en/latest/data/api/llm.html)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "The `ray.data.llm` module integrates with key large language model (LLM) inference engines and deployed models to enable LLM batch inference. These LLM modules use [Ray Data](https://docs.ray.io/en/latest/data/data.html) under the hood, which makes it extremely easy to distribute workloads but also ensures that they happen:\n",
    "- **efficiently**: minimizing CPU/GPU idle time with heterogeneous resource scheduling.\n",
    "- **at scale**: with streaming execution to petabyte-scale datasets, especially when [working with LLMs](https://docs.ray.io/en/latest/data/working-with-llms.html).\n",
    "- **reliably** by checkpointing processes, especially when running workloads on spot instances with on-demand fallback.\n",
    "- **flexibly**: connecting to data from any source, applying transformations, and saving to any format and location for your next workload.\n",
    "\n",
    "<img src=\"https://raw.githubusercontent.com/anyscale/foundational-ray-app/refs/heads/main/images/ray_data_solution.png\" width=800>\n",
    "\n",
    "[RayTurbo Data](https://docs.anyscale.com/rayturbo/rayturbo-data) has more features on top of Ray Data:\n",
    "- **accelerated metadata fetching** to improve reading first time from large datasets \n",
    "- **optimized autoscaling** where Jobs can kick off before waiting for the entire cluster to start\n",
    "- **high reliability** where entire failed jobs, like head node, cluster, uncaptured exceptions, etc., can resume from checkpoints. OSS Ray can only recover from worker node failures."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Start by defining the [vLLM engine processor config](https://docs.ray.io/en/latest/data/api/doc/ray.data.llm.vLLMEngineProcessorConfig.html#ray.data.llm.vLLMEngineProcessorConfig) where you can select the model to use and the [engine behavior](https://docs.vllm.ai/en/stable/serving/engine_args.html). The model can come from [Hugging Face (HF) Hub](https://huggingface.co/models) or a local model path `/path/to/your/model`. Anyscale supports GPTQ, GGUF, or LoRA model formats.\n",
    "\n",
    "<img src=\"https://raw.githubusercontent.com/anyscale/e2e-llm-workflows/refs/heads/main/images/data_llm.png\" width=800>"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### vLLM engine processor"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "INFO 04-11 14:58:40 __init__.py:194] No platform detected, vLLM is running on UnspecifiedPlatform\n"
     ]
    }
   ],
   "source": [
    "import os\n",
    "import ray\n",
    "from ray.data.llm import vLLMEngineProcessorConfig"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "config = vLLMEngineProcessorConfig(\n",
    "    model_source=model_source,\n",
    "    runtime_env={\n",
    "        \"env_vars\": {\n",
    "            \"VLLM_USE_V1\": \"0\",  # v1 doesn't support lora adapters yet\n",
    "            # \"HF_TOKEN\": os.environ.get(\"HF_TOKEN\"),\n",
    "        },\n",
    "    },\n",
    "    engine_kwargs={\n",
    "        \"enable_lora\": True,\n",
    "        \"max_lora_rank\": 8,\n",
    "        \"max_loras\": 1,\n",
    "        \"pipeline_parallel_size\": 1,\n",
    "        \"tensor_parallel_size\": 1,\n",
    "        \"enable_prefix_caching\": True,\n",
    "        \"enable_chunked_prefill\": True,\n",
    "        \"max_num_batched_tokens\": 4096,\n",
    "        \"max_model_len\": 4096,  # or increase KV cache size\n",
    "        # complete list: https://docs.vllm.ai/en/stable/serving/engine_args.html\n",
    "    },\n",
    "    concurrency=1,\n",
    "    batch_size=16,\n",
    "    accelerator_type=\"L4\",\n",
    ")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### LLM processor"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Next, pass the config to an [LLM processor](https://docs.ray.io/en/master/data/api/doc/ray.data.llm.build_llm_processor.html#ray.data.llm.build_llm_processor) where you can define the preprocessing and postprocessing steps around inference. With your base model defined in the processor config, you can define the LoRA adapter layers as part of the preprocessing step of the LLM processor itself."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "from ray.data.llm import build_llm_processor"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "2025-04-11 14:58:40,942\tINFO worker.py:1660 -- Connecting to existing Ray cluster at address: 10.0.51.51:6379...\n",
      "2025-04-11 14:58:40,953\tINFO worker.py:1843 -- Connected to Ray cluster. View the dashboard at \u001b[1m\u001b[32mhttps://session-zt5t77xa58pyp3uy28glg2g24d.i.anyscaleuserdata.com \u001b[39m\u001b[22m\n",
      "2025-04-11 14:58:40,960\tINFO packaging.py:367 -- Pushing file package 'gcs://_ray_pkg_e71d58b4dc01d065456a9fc0325ee2682e13de88.zip' (2.16MiB) to Ray cluster...\n",
      "2025-04-11 14:58:40,969\tINFO packaging.py:380 -- Successfully pushed file package 'gcs://_ray_pkg_e71d58b4dc01d065456a9fc0325ee2682e13de88.zip'.\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "a9171027a5a249ff801e77f763506f67",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "config.json:   0%|          | 0.00/663 [00:00<?, ?B/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\u001b[36m(pid=51260)\u001b[0m INFO 04-11 14:58:47 __init__.py:194] No platform detected, vLLM is running on UnspecifiedPlatform\n"
     ]
    }
   ],
   "source": [
    "processor = build_llm_processor(\n",
    "    config,\n",
    "    preprocess=lambda row: dict(\n",
    "        model=lora_path,  # REMOVE this line if doing inference with just the base model\n",
    "        messages=[\n",
    "            {\"role\": \"system\", \"content\": system_content},\n",
    "            {\"role\": \"user\", \"content\": row[\"input\"]}\n",
    "        ],\n",
    "        sampling_params={\n",
    "            \"temperature\": 0.3,\n",
    "            \"max_tokens\": 250,\n",
    "            # complete list: https://docs.vllm.ai/en/stable/api/inference_params.html\n",
    "        },\n",
    "    ),\n",
    "    postprocess=lambda row: {\n",
    "        **row,  # all contents\n",
    "        \"generated_output\": row[\"generated_text\"],\n",
    "        # add additional outputs\n",
    "    },\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "\n",
      "{\n",
      "  \"batch_uuid\": \"d7a6b5341cbf4986bb7506ff277cc9cf\",\n",
      "  \"embeddings\": null,\n",
      "  \"generated_text\": \"request(esrb)\",\n",
      "  \"generated_tokens\": [2035, 50236, 10681, 8, 151645],\n",
      "  \"input\": \"Do you have a favorite ESRB content rating?\",\n",
      "  \"instruction\": \"Given a target sentence construct the underlying meaning representation of the input sentence as a single function with attributes and attribute values. This function should describe the target string accurately and the function must be one of the following ['inform', 'request', 'give_opinion', 'confirm', 'verify_attribute', 'suggest', 'request_explanation', 'recommend', 'request_attribute']. The attributes must be one of the following: ['name', 'exp_release_date', 'release_year', 'developer', 'esrb', 'rating', 'genres', 'player_perspective', 'has_multiplayer', 'platforms', 'available_on_steam', 'has_linux_release', 'has_mac_release', 'specifier']\",\n",
      "  \"messages\": [\n",
      "    {\n",
      "      \"content\": \"Given a target sentence construct the underlying meaning representation of the input sentence as a single function with attributes and attribute values. This function should describe the target string accurately and the function must be one of the following ['inform', 'request', 'give_opinion', 'confirm', 'verify_attribute', 'suggest', 'request_explanation', 'recommend', 'request_attribute']. The attributes must be one of the following: ['name', 'exp_release_date', 'release_year', 'developer', 'esrb', 'rating', 'genres', 'player_perspective', 'has_multiplayer', 'platforms', 'available_on_steam', 'has_linux_release', 'has_mac_release', 'specifier']\",\n",
      "      \"role\": \"system\"\n",
      "    },\n",
      "    {\n",
      "      \"content\": \"Do you have a favorite ESRB content rating?\",\n",
      "      \"role\": \"user\"\n",
      "    }\n",
      "  ],\n",
      "  \"metrics\": {\n",
      "    \"arrival_time\": 1744408857.148983,\n",
      "    \"finished_time\": 1744408863.09091,\n",
      "    \"first_scheduled_time\": 1744408859.130259,\n",
      "    \"first_token_time\": 1744408862.7087252,\n",
      "    \"last_token_time\": 1744408863.089174,\n",
      "    \"model_execute_time\": null,\n",
      "    \"model_forward_time\": null,\n",
      "    \"scheduler_time\": 0.04162892400017881,\n",
      "    \"time_in_queue\": 1.981276035308838\n",
      "  },\n",
      "  \"model\": \"/mnt/cluster_storage/viggo/saves/lora_sft_ray/TorchTrainer_95d16_00000_0_2025-04-11_14-47-37/checkpoint_000000/checkpoint\",\n",
      "  \"num_generated_tokens\": 5,\n",
      "  \"num_input_tokens\": 164,\n",
      "  \"output\": \"request_attribute(esrb[])\",\n",
      "  \"params\": \"SamplingParams(n=1, presence_penalty=0.0, frequency_penalty=0.0, repetition_penalty=1.0, temperature=0.3, top_p=1.0, top_k=-1, min_p=0.0, seed=None, stop=[], stop_token_ids=[], bad_words=[], include_stop_str_in_output=False, ignore_eos=False, max_tokens=250, min_tokens=0, logprobs=None, prompt_logprobs=None, skip_special_tokens=True, spaces_between_special_tokens=True, truncate_prompt_tokens=None, guided_decoding=None)\",\n",
      "  \"prompt\": \"<|im_start|>system\n",
      "Given a target sentence construct the underlying meaning representation of the input sentence as a single function with attributes and attribute values. This function should describe the target string accurately and the function must be one of the following ['inform', 'request', 'give_opinion', 'confirm', 'verify_attribute', 'suggest', 'request_explanation', 'recommend', 'request_attribute']. The attributes must be one of the following: ['name', 'exp_release_date', 'release_year', 'developer', 'esrb', 'rating', 'genres', 'player_perspective', 'has_multiplayer', 'platforms', 'available_on_steam', 'has_linux_release', 'has_mac_release', 'specifier']<|im_end|>\n",
      "<|im_start|>user\n",
      "Do you have a favorite ESRB content rating?<|im_end|>\n",
      "<|im_start|>assistant\n",
      "\",\n",
      "  \"prompt_token_ids\": [151644, \"...\", 198],\n",
      "  \"request_id\": 94,\n",
      "  \"time_taken_llm\": 6.028705836999961,\n",
      "  \"generated_output\": \"request(esrb)\"\n",
      "}\n",
      "\n",
      "\n"
     ]
    }
   ],
   "source": [
    "# Evaluation on test dataset\n",
    "ds = ray.data.read_json(\"/mnt/cluster_storage/viggo/test.jsonl\")  # complete list: https://docs.ray.io/en/latest/data/api/input_output.html\n",
    "ds = processor(ds)\n",
    "results = ds.take_all()\n",
    "results[0]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "0.6879039704524469"
      ]
     },
     "execution_count": null,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Exact match (strict!)\n",
    "matches = 0\n",
    "for item in results:\n",
    "    if item[\"output\"] == item[\"generated_output\"]:\n",
    "        matches += 1\n",
    "matches / float(len(results))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "**Note**: The objective of fine-tuning here isn't to create the most performant model but to show that you can leverage it for downstream workloads, like batch inference and online serving at scale. However, you can increase `num_train_epochs` if you want to."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Observe the individual steps in the batch inference workload through the Anyscale Ray Data dashboard:\n",
    "\n",
    "<img src=\"https://raw.githubusercontent.com/anyscale/e2e-llm-workflows/refs/heads/main/images/data_dashboard.png\" width=1000>"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "<div class=\"alert alert-info\">\n",
    "\n",
    "💡 For more advanced guides on topics like optimized model loading, multi-LoRA, OpenAI-compatible endpoints, etc., see [more examples](https://docs.ray.io/en/latest/data/working-with-llms.html) and the [API reference](https://docs.ray.io/en/latest/data/api/llm.html).\n",
    "\n",
    "</div>"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Online serving\n",
    "[`Overview`](https://docs.ray.io/en/latest/serve/llm/serving-llms.html) | [`API reference`](https://docs.ray.io/en/latest/serve/api/index.html#llm-api)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "<img src=\"https://raw.githubusercontent.com/anyscale/foundational-ray-app/refs/heads/main/images/ray_serve.png\" width=600>\n",
    "\n",
    "`ray.serve.llm` APIs allow users to deploy multiple LLM models together with a familiar Ray Serve API, while providing compatibility with the OpenAI API.\n",
    "\n",
    "<img src=\"https://raw.githubusercontent.com/anyscale/e2e-llm-workflows/refs/heads/main/images/serve_llm.png\" width=500>\n",
    "\n",
    "Ray Serve LLM is designed with the following features:\n",
    "- Automatic scaling and load balancing\n",
    "- Unified multi-node multi-model deployment\n",
    "- OpenAI compatibility\n",
    "- Multi-LoRA support with shared base models\n",
    "- Deep integration with inference engines, vLLM to start\n",
    "- Composable multi-model LLM pipelines\n",
    "\n",
    "[RayTurbo Serve](https://docs.anyscale.com/rayturbo/rayturbo-serve) on Anyscale has more features on top of Ray Serve:\n",
    "- **fast autoscaling and model loading** to get services up and running even faster: [5x improvements](https://www.anyscale.com/blog/autoscale-large-ai-models-faster) even for LLMs\n",
    "- 54% **higher QPS** and up-to 3x **streaming tokens per second** for high traffic serving use-cases\n",
    "- **replica compaction** into fewer nodes where possible to reduce resource fragmentation and improve hardware utilization\n",
    "- **zero-downtime** [incremental rollouts](https://docs.anyscale.com/platform/services/update-a-service/#resource-constrained-updates) so your service is never interrupted\n",
    "- [**different environments**](https://docs.anyscale.com/platform/services/multi-app/#multiple-applications-in-different-containers) for each service in a multi-serve application\n",
    "- **multi availability-zone** aware scheduling of Ray Serve replicas to provide higher redundancy to availability zone failures\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### LLM serve config"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "import os\n",
    "from openai import OpenAI  # to use openai api format\n",
    "from ray import serve\n",
    "from ray.serve.llm import LLMConfig, build_openai_app"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Define an [LLM config](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.LLMConfig.html#ray.serve.llm.LLMConfig) where you can define where the model comes from, its [autoscaling behavior](https://docs.ray.io/en/latest/serve/autoscaling-guide.html#serve-autoscaling), what hardware to use and [engine arguments](https://docs.vllm.ai/en/stable/serving/engine_args.html).\n",
    "\n",
    "**Note**: If you're using AWS S3, replace `AWS_REGION` in the `runtime_env`'s `env_vars` below with the cloud storage and respective region you saved your model artifacts to. Do the same if using other cloud storage options as well."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Define config.\n",
    "llm_config = LLMConfig(\n",
    "    model_loading_config={\n",
    "        \"model_id\": model_id,\n",
    "        \"model_source\": model_source\n",
    "    },\n",
    "    lora_config={  # REMOVE this section if you're only using a base model.\n",
    "        \"dynamic_lora_loading_path\": dynamic_lora_path,\n",
    "        \"max_num_adapters_per_replica\": 16,  # You only have 1.\n",
    "    },\n",
    "    runtime_env={\"env_vars\": {\"AWS_REGION\": \"us-west-2\"}},\n",
    "    # runtime_env={\"env_vars\": {\"HF_TOKEN\": os.environ.get(\"HF_TOKEN\")}},\n",
    "    deployment_config={\n",
    "        \"autoscaling_config\": {\n",
    "            \"min_replicas\": 1,\n",
    "            \"max_replicas\": 2,\n",
    "            # complete list: https://docs.ray.io/en/latest/serve/autoscaling-guide.html#serve-autoscaling\n",
    "        }\n",
    "    },\n",
    "    accelerator_type=\"L4\",\n",
    "    engine_kwargs={\n",
    "        \"max_model_len\": 4096,  # Or increase KV cache size.\n",
    "        \"tensor_parallel_size\": 1,\n",
    "        \"enable_lora\": True,\n",
    "        # complete list: https://docs.vllm.ai/en/stable/serving/engine_args.html\n",
    "    },\n",
    ")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Now deploy the LLM config as an application. And because this application is all built on top of [Ray Serve](https://docs.ray.io/en/latest/serve/index.html), you can have advanced service logic around composing models together, deploying multiple applications, model multiplexing, observability, etc."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "DeploymentHandle(deployment='LLMRouter')\n"
     ]
    }
   ],
   "source": [
    "# Deploy.\n",
    "app = build_openai_app({\"llm_configs\": [llm_config]})\n",
    "serve.run(app)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### Service request"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "\n",
      "Avg prompt throughput: 20.3 tokens/s, Avg generation throughput: 0.1 tokens/s, Running: 1 reqs, Swapped: 0 reqs, Pending: 0 reqs, GPU KV cache usage: 0.3%, CPU KV cache usage: 0.0%.\n",
      "\n",
      "_opinion(name[Diablo II], developer[Blizzard North], rating[good], has_mac_release[yes])\n",
      "\n",
      "\n"
     ]
    }
   ],
   "source": [
    "# Initialize client.\n",
    "client = OpenAI(base_url=\"http://localhost:8000/v1\", api_key=\"fake-key\")\n",
    "response = client.chat.completions.create(\n",
    "    model=f\"{model_id}:{lora_id}\",\n",
    "    messages=[\n",
    "        {\"role\": \"system\", \"content\": \"Given a target sentence construct the underlying meaning representation of the input sentence as a single function with attributes and attribute values. This function should describe the target string accurately and the function must be one of the following ['inform', 'request', 'give_opinion', 'confirm', 'verify_attribute', 'suggest', 'request_explanation', 'recommend', 'request_attribute']. The attributes must be one of the following: ['name', 'exp_release_date', 'release_year', 'developer', 'esrb', 'rating', 'genres', 'player_perspective', 'has_multiplayer', 'platforms', 'available_on_steam', 'has_linux_release', 'has_mac_release', 'specifier']\"},\n",
    "        {\"role\": \"user\", \"content\": \"Blizzard North is mostly an okay developer, but they released Diablo II for the Mac and so that pushes the game from okay to good in my view.\"},\n",
    "    ],\n",
    "    stream=True\n",
    ")\n",
    "for chunk in response:\n",
    "    if chunk.choices[0].delta.content is not None:\n",
    "        print(chunk.choices[0].delta.content, end=\"\", flush=True)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "And of course, you can observe the running service, the deployments, and metrics like QPS, latency, etc., through the [Ray Dashboard](https://docs.ray.io/en/latest/ray-observability/getting-started.html)'s [Serve view](https://docs.ray.io/en/latest/ray-observability/getting-started.html#dash-serve-view):\n",
    "\n",
    "<img src=\"https://raw.githubusercontent.com/anyscale/e2e-llm-workflows/refs/heads/main/images/serve_dashboard.png\" width=1000>"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "<div class=\"alert alert-info\">\n",
    "\n",
    "💡 See [more examples](https://docs.ray.io/en/latest/serve/llm/serving-llms.html) and the [API reference](https://docs.ray.io/en/latest/serve/llm/api.html) for advanced guides on topics like structured outputs (like JSON), vision LMs, multi-LoRA on shared base models, using other inference engines (like `sglang`), fast model loading, etc.\n",
    "\n",
    "</div>"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Production\n",
    "\n",
    "Seamlessly integrate with your existing CI/CD pipelines by leveraging the Anyscale [CLI](https://docs.anyscale.com/reference/quickstart-cli) or [SDK](https://docs.anyscale.com/reference/quickstart-sdk) to run [reliable batch jobs](https://docs.anyscale.com/platform/jobs) and deploy [highly available services](https://docs.anyscale.com/platform/services). Given you've been developing in an environment that's almost identical to production with a multi-node cluster, this integration should drastically speed up your dev to prod velocity.\n",
    "\n",
    "<img src=\"https://raw.githubusercontent.com/anyscale/foundational-ray-app/refs/heads/main/images/cicd.png\" width=600>\n",
    "\n",
    "### Jobs\n",
    "\n",
    "[Anyscale Jobs](https://docs.anyscale.com/platform/jobs/) ([API ref](https://docs.anyscale.com/reference/job-api/)) allows you to execute discrete workloads in production such as batch inference, embeddings generation, or model fine-tuning.\n",
    "- [define and manage](https://docs.anyscale.com/platform/jobs/manage-jobs) your Jobs in many different ways, like CLI and Python SDK\n",
    "- set up [queues](https://docs.anyscale.com/platform/jobs/job-queues) and [schedules](https://docs.anyscale.com/platform/jobs/schedules)\n",
    "- set up all the [observability, alerting, etc.](https://docs.anyscale.com/platform/jobs/monitoring-and-debugging) around your Jobs\n",
    "\n",
    "<img src=\"https://raw.githubusercontent.com/anyscale/foundational-ray-app/refs/heads/main/images/job_result.png\" width=700>\n",
    "\n",
    "### Services\n",
    "\n",
    "[Anyscale Services](https://docs.anyscale.com/platform/services/) ([API ref](https://docs.anyscale.com/reference/service-api/)) offers an extremely fault tolerant, scalable, and optimized way to serve your Ray Serve applications:\n",
    "- you can [rollout and update](https://docs.anyscale.com/platform/services/update-a-service) services with canary deployment with zero-downtime upgrades\n",
    "- [monitor](https://docs.anyscale.com/platform/services/monitoring) your Services through a dedicated Service page, unified log viewer, tracing, set up alerts, etc.\n",
    "- scale a service (`num_replicas=auto`) and utilize replica compaction to consolidate nodes that are fractionally utilized\n",
    "- [head node fault tolerance](https://docs.anyscale.com/platform/services/production-best-practices#head-node-ft) because OSS Ray recovers from failed workers and replicas but not head node crashes\n",
    "- serving [multiple applications](https://docs.anyscale.com/platform/services/multi-app) in a single Service\n",
    "\n",
    "<img src=\"https://raw.githubusercontent.com/anyscale/foundational-ray-app/refs/heads/main/images/canary.png\" width=700>\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "%%bash\n",
    "# clean up\n",
    "rm -rf /mnt/cluster_storage/viggo\n",
    "STORAGE_PATH=\"$ANYSCALE_ARTIFACT_STORAGE/viggo\"\n",
    "if [[ \"$STORAGE_PATH\" == s3://* ]]; then\n",
    "    aws s3 rm \"$STORAGE_PATH\" --recursive --quiet\n",
    "elif [[ \"$STORAGE_PATH\" == gs://* ]]; then\n",
    "    gsutil -m -q rm -r \"$STORAGE_PATH\"\n",
    "fi"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "base",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.11"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
