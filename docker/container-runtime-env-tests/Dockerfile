FROM rayproject/ray:nightly-py38-cpu

# Downgrade python to 3.8.16
RUN conda install python=3.8.16 -y

# Custom file
RUN echo "Hello world ABC" > /home/ray/file.txt
