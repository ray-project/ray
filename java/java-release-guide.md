# Ray Java Release Guideline

This document help Ray Java developers release Ray Java.  
Note that we assume you release it on your local laptop.

## 1. Installing GPG
**If you have installed GPG, you can skip this step.**  

Install gpg software on your laptop. https://gpgtools.org/  
Create your own key pair of GPG.  
Distribute it to a key server so that users can validate it.(Follow the guide of GPG software)
  
Post a PR to bump the version to the release branch and merge it.  
Change all of the version numbers of `pom.xml` files and `pom_template.xml` files. (These files are under `java/`directory)


## 2. Deploying to Maven Central Repo

**Make sure you are under the Ray root source directory.**
```bash
export OSSRH_KEY=xxx
export OSSRH_TOKEN=xxx
export TRAVIS_BRANCH=releases/x.y.z  # Your releasing version number
export TRAVIS_COMMIT=xxxxxxxxxxx     # The commit id
git checkout $TRAVIS_COMMIT
sh java/build-jar-multiplatform.sh multiplatform
export GPG_SKIP=false
cd java && mvn versions:set -DnewVersion=x.y.z && cd -
sh java/build-jar-multiplatform.sh deploy_jars
```

Note that you should set the **TRAVIS_BRANCH** and **TRAVIS_COMMIT** to the correct values.

This is an example version(ray-1.4.0) we have released:
```bash
export TRAVIS_BRANCH=releases/1.4.0
export TRAVIS_COMMIT=3a09c82fbfce8f00533234844729e6d99fb0f24c
git checkout $TRAVIS_COMMIT
sh java/build-jar-multiplatform.sh multiplatform
export GPG_SKIP=false
cd java && mvn versions:set -DnewVersion=1.4.0 && cd -
sh java/build-jar-multiplatform.sh deploy_jars
```

## 3. Closing and Releasing it

Login to the [SONATYPE](https://oss.sonatype.org/) website and there are 2 buttons you should click:  
- First click the `Close` button. It’ll validate your releasing jars.  
- Then click the `Release` button.
