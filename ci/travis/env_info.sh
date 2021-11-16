#!/bin/bash

echo "Test environment information"
echo "----------------------------"
echo "Python version: $(python --version 2>/dev/null || echo 'Python not installed')"
echo "Ray version: $(ray --version 2>/dev/null || echo 'Ray not installed')"
echo "Installed pip packages:"
python -m pip freeze 2>/dev/null || echo 'Pip not installed'
echo "----------------------------"

echo "GPU information"
echo "----------------------------"
GPUCMD="nvidia-smi"
if ! command -v "${GPUCMD}" &> /dev/null
then
    echo "No GPU support found (${GPUCMD} not found)."
else
    eval "${GPUCMD}"
    python -c "import torch; print('Torch cuda available:', torch.cuda.is_available())"
fi
echo "----------------------------"
