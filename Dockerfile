FROM nvidia/cuda:12.6.0-base-ubuntu22.04

RUN apt-get update && apt-get install -y --no-install-recommends \
	build-essential \
	cmake \
	git \
	wget \
	curl \
	zsh \
	vim \
	python3 \
	python3-pip \
	python3-dev \
	python3-setuptools \
    python3-venv \
	&& rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y build-essential curl clang-12 pkg-config psmisc unzip

# Install Oh My Zsh
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended

# Install zsh-autosuggestions plugin
RUN git clone https://github.com/zsh-users/zsh-autosuggestions ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-autosuggestions

# Configure zsh with autosuggestions
RUN echo 'plugins=(git zsh-autosuggestions)' >> ~/.zshrc && \
    echo 'ZSH_THEME="robbyrussell"' >> ~/.zshrc

# Set Zsh as default shell
RUN chsh -s $(which zsh)

WORKDIR /app

CMD ["zsh"]
