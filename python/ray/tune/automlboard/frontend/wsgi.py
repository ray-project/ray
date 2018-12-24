"""
WSGI config for monitor project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/1.11/howto/deployment/wsgi/

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from django.core.wsgi import get_wsgi_application

import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE",
                      "ray.tune.automlboard.settings")
application = get_wsgi_application()
