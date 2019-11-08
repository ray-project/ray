from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from skein import ApplicationClient


def main():
    app = ApplicationClient.from_current()
    app.add_container("ray_worker")


if __name__ == '__main__':
    main()
