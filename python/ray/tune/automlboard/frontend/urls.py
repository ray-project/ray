"""
Monitor URL Configuration.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.11/topics/http/urls/

Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')

Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')

Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from django.conf.urls import url
from django.contrib import admin

import ray.tune.automlboard.frontend.view as view
import ray.tune.automlboard.frontend.query as query

urlpatterns = [
    url(r"^admin/", admin.site.urls),
    url(r"^$", view.index),
    url(r"^job$", view.job),
    url(r"^trial$", view.trial),
    url(r"^query_job", query.query_job),
    url(r"^query_trial", query.query_trial)
]
