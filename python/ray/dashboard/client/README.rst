This project was bootstrapped with `Create React App
<https://github.com/facebook/create-react-app>`__.

Available Scripts
-----------------

In the project directory, you can run:

``npm ci``
~~~~~~~~~~

Once you run this command, all the dependencies listed in your package.json are installed based on the versions in package-lock.json file.

``npm start``
~~~~~~~~~~~~~

Runs the app in the development mode. Open `http://localhost:3000
<http://localhost:3000>`__ to view it in the browser.

The page will reload if you make edits. You will also see any lint errors in the
console.

Handling CORS Errors During Local Development
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When developing locally, you may encounter CORS (Cross-Origin Resource Sharing) errors. This is a browser security feature that prevents network connections when the domain name doesn't match the expected one. In this case, ``localhost`` is the domain name but ``console.anyscale.com`` (or your deployment domain) is expected.

To work around this during local development, you can launch Chrome with web security disabled:

**On macOS:**

.. code-block:: bash

   open -n -a /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --args --user-data-dir="/tmp/chrome_dev_test" --disable-web-security

**On Linux:**

.. code-block:: bash

   google-chrome --user-data-dir="/tmp/chrome_dev_test" --disable-web-security

**On Windows:**

.. code-block:: bash

   "C:\Program Files\Google\Chrome\Application\chrome.exe" --user-data-dir="C:\tmp\chrome_dev_test" --disable-web-security

**Important Security Note:** CORS protection is there to protect the user, not the server. When running with web security disabled, only visit trusted sites (like your local development server). Do not use this browser instance for general web browsing, as it removes important security protections.

``npm test``
~~~~~~~~~~~~

Launches the test runner in the interactive watch mode. See the section about
`running tests
<https://facebook.github.io/create-react-app/docs/running-tests>`__ for more
information.

``npm run build``
~~~~~~~~~~~~~~~~~

Builds the app for production to the ``build`` folder. It correctly bundles
React in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes. Your app is ready to
be deployed!

See the section about `deployment
<https://facebook.github.io/create-react-app/docs/deployment>`__ for more
information.

``npm run eject``
~~~~~~~~~~~~~~~~~

Note: this is a one-way operation. Once you ``eject``, you can’t go back!

If you aren’t satisfied with the build tool and configuration choices, you can
``eject`` at any time. This command will remove the single build dependency from
your project.

Instead, it will copy all the configuration files and the transitive
dependencies (Webpack, Babel, ESLint, etc) right into your project so you have
full control over them. All of the commands except ``eject`` will still work,
but they will point to the copied scripts so you can tweak them. At this point
you’re on your own.

You don’t have to ever use ``eject``. The curated feature set is suitable for
small and middle deployments, and you shouldn’t feel obligated to use this
feature. However we understand that this tool wouldn’t be useful if you couldn’t
customize it when you are ready for it.

Learn More
----------

You can learn more in the `Create React App documentation
<https://facebook.github.io/create-react-app/docs/getting-started>`__.

To learn React, check out the `React documentation <https://reactjs.org/>`__.
