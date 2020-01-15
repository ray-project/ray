# Ray Website

## Development instructions

With Ruby >= 2.1 installed, run:

```
gem install jekyll bundler
bundle install
```

To view the site, run:

```
bundle exec jekyll serve
```

To view the site with Google Analytics, run:

```
JEKYLL_ENV=production bundle exec jekyll serve
```

## Deployment

To deploy the site, run

```
cd ~
git clone git@github.com:ray-project/ray-project.github.io.git
cd ray-project.github.io
cp -r ~/ray/site/* .
```

and commit as well as push the desired changes.
