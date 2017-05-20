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

## Deployment

To deploy the site, run (inside the main ray directory):

```
git subtree push --prefix site origin gh-pages
```
