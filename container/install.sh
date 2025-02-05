#!/bin/zsh

# Init Python virtual environment
cd /app
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip wheel
echo 'source /app/venv/bin/activate' >>~/.zshrc

# Instal bazel
/app/ci/env/install-bazel.sh --user
echo 'export PATH="$PATH:$HOME/bin"' >>~/.zshrc
source ~/.zshrc
bazel --version

# Install Node.js
cd
wget https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh
chmod +x install.sh
./install.sh
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"
rm install.sh
nvm install 14
nvm use 14
cd /app/python/ray/dashboard/client
npm ci
npm run build

# Install Python dependencies
cd /app/python
pip install -r requirements.txt
pip install -e . --verbose
pip install pytest torch
pip install -c requirements_compiled.txt -r requirements/lint-requirements.txt
