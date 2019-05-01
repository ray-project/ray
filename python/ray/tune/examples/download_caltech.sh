cd ~
mkdir data
mkdir data/caltech
cd data/caltech
wget http://www.vision.caltech.edu/Image_Datasets/Caltech256/256_ObjectCategories.tar
tar -xvf 256_ObjectCategories.tar -C ./
mv 256_ObjectCategories Images
rm 256_ObjectCategories.tar
