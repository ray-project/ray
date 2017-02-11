# The Ray Web UI

This is Ray's Web UI. It consists of two components:

* The **frontend** is a [Polymer](https://www.polymer-project.org/1.0/) app that
  uses [D3](https://d3js.org/) for visualization.
* The **backend** is a Python 3 websocket server (see `backend/ray_ui.py`) that
  connects to Redis and potentially Ray.

### Prerequisites

The Ray Web UI requires Python 3.

Install [polymer-cli](https://github.com/Polymer/polymer-cli):

    pip install aioredis websockets
    npm install -g polymer-cli

### Setup

The following must be done once.

    cd webui
    bower install

### Start the backend

First start Ray and note down the address of the Redis server. Then run

    cd webui/backend
    python ray_ui.py --redis-address 127.0.0.1:6379

where you substitute your Redis address appropriately.

### Start the frontend development server

To start the front end, run the following.

    cd webui
    polymer serve --open

The web UI can then be accessed at `http://localhost:8080`.

### Build

This command performs HTML, CSS, and JS minification on the application
dependencies, and generates a service-worker.js file with code to pre-cache the
dependencies based on the entrypoint and fragments specified in `polymer.json`.
The minified files are output to the `build/unbundled` folder, and are suitable
for serving from a HTTP/2+Push compatible server.

In addition the command also creates a fallback `build/bundled` folder,
generated using fragment bundling, suitable for serving from non
H2/push-compatible servers or to clients that do not support H2/Push.

    polymer build

### Preview the build

This command serves the minified version of the app at `http://localhost:8080`
in an unbundled state, as it would be served by a push-compatible server:

    polymer serve build/unbundled

This command serves the minified version of the app at `http://localhost:8080`
generated using fragment bundling:

    polymer serve build/bundled

### Run tests

This command will run
[Web Component Tester](https://github.com/Polymer/web-component-tester) against
the browsers currently installed on your machine.

    polymer test

### Adding a new view

You can extend the app by adding more views that will be demand-loaded e.g.
based on the route, or to progressively render non-critical sections of the
application.  Each new demand-loaded fragment should be added to the list of
`fragments` in the included `polymer.json` file.  This will ensure those
components and their dependencies are added to the list of pre-cached components
(and will have bundles created in the fallback `bundled` build).
