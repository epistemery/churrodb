import sys
from setuptools import setup

VERSION = "0.1"

requires = ["churro"]
tests_require = [] + requires
# tests_require = requires + ["testfixtures"]
testing_extras = tests_require + ["coverage"]

if sys.version < "2.7":
    tests_require += ["unittest2"]

if sys.version < "3.3":
    tests_require += ["mock"]

setup(name="churrodb",
      version="0.1",
      license="GPL",
      author="Stefan Frühwirth",
      author_email="stefan.fruehwirth@uni-graz.at",
      install_requires=requires,
      tests_require=tests_require,
      extras_require={"testing": testing_extras},
      packages=["churrodb"],
      test_suite="churrodb.tests")
