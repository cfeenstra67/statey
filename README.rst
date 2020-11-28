############
Statey
############

Introduction
#############

Statey is an infrastructure-as-code framework written in Python. The API is designed to be as expressive and Pythonic as possible, and the simplicity of the design makes it just as easy to embed in a Python application as it is to use the command line interface like any infra-as-code application.

Statey supports pulumi resource providers through `pylumi <https://github.com/cfeenstra67/pylumi>`_, which actually in turn supports Terraform resource providers. This means an entire package index of potential resources is available*.

*_NOTE_: Terraform resource providers communicate their type information via `JSONSchema`, which supports higher-level type operations such as `oneOf`. At the moment `statey` does not support anything except basic `JSONSchema` types (objects, strings, numbers, arrays, booleans, etc.)

The core engine is lightweight and has just a few pure-python dependencies, and it is built from the ground up for extensibility. One of the core data strucrues in statey is the `Registry`, which is built on top of the excellent plugin engine `pluggy <https://github.com/pytest-dev/pluggy>`_. Nearly all of the core functions of statey are hook-based and can be extended or overridden very easily.

Installation
#############

`statey` and all of its core extension modules can be installed using the following terminal command:

.. code-block:: bash

   $ pip install statey[all]

_NOTE_: If you are using Zshell you will have to put `statey[all]` in quotes i.e. `"statey[all]"`.

To install only the core `statey` engine the command is:

.. code-block:: bash

	$ pip install statey

There are a number of possible `extras` to install, depending on the use-case:

- `fmt` - This enables the `st.f('{some_ref.abc} and other things')` format-string function.
- `pickle` - This adds extra `pickle` extension packages so that functions can be more reliably pickled.
- `cli` - This include requirements required to run the statey CLI.
- `pulumi` - This includes requirements to utilize pulumi resourc providers.
- `all` - This includes requirements from all of the above.
- `core` - This includes the requirements from the `fmt` and `cli` extras.
- `tests` - This includes requirements to run the tests.
- `dev` - This includes miscellaneous requirements required for development such as `black`.

For most users, installing `statey[all]` will be the proper entry point.

Usage Example
###############

**NOTE**: to run this example you must have the pulumi aws provider installed. This can be done by running the following if it is not already installed:

.. code-block:: bash

	$ statey install pulumi/aws==2.13.1

A typical statey module might look something like the following (in a file called `statey_module.py`):

.. code-block:: python

	import statey as st
	from statey.ext.pulumi.providers import aws

	@st.declarative
	def module(session):
		bucket = aws.s3.Bucket(
			bucket='my-bucket-name'
		)
		object_1 = aws.s3.BucketObject(
			bucket=bucket.bucket,
			key='file-1.json',
			source='./static/file1.json',
			contentType='application/json'
		)
		object_2 = aws.s3.BucketObject(
			bucket=bucket.bucket,
			key='file-2.txt',
			content=st.f('This is in a bucket named {bucket.bucket}')
		)

Next, simply run the following in the same directory as your `statey_module.py` file:

.. code-block:: bash

	$ export AWS_DEFAULT_REGION=<my_default_region>
	$ statey up

The `export AWS_DEFAULT_REGION` command is essential because setting the region is required for the Pulumi AWS provider. As an alternative and more general solution to statey configuration one could create a `statey_conf.py` file in the same directory with the following content:

.. code-block:: python
	
	import statey as st

	st.helpers.set_provider_defaults("pulumi/aws", {"region": "<my_default_region>"})

The conf file will always be run before the `statey_module.py` module is loaded, and it is intended to register hooks to change statey's behavior.

After running `statey up`, the application will display a confirmation message, and if confirmed will subsequently execute the operations displayed in the plan. At this point the `statey` application is fully aware of and managing the infrastructure defined in `statey_module.py`. You can edit, remove, add to or delete this infrastructure fluently and incrementally without interrupting your existing resources. For example, perhaps we want to change the naming scheme for our s3 objects:

.. code-block:: python

	import statey as st
	from statey.ext.pulumi.providers import aws

	@st.declarative
	def module(session):
		bucket = aws.s3.Bucket(
			bucket='my-bucket-name'
		)
		object_1 = aws.s3.BucketObject(
			bucket=bucket.bucket,
			key='statey-test-file-1.json',
			source='./static/file1.json',
			contentType='application/json'
		)
		object_2 = aws.s3.BucketObject(
			bucket=bucket.bucket,
			key='statey-test-file-2.txt',
			content=st.f('This is in a bucket named {bucket.bucket}')
		)

You should get an output something like the following:

.. code-block:: bash

	* object_2:current:task:delete            
	| * object_1:current:task:delete             
	* | object_2:config:task:create                                           
	 /                
	* object_1:config:task:create  

Since you are changing the key of each object, `statey` detects that each one needs to be deleted and recreated, and understands the order those things need to be done in. The same goes for any update you make to your configuration, or tearing down all of your infrastructure altogether.

Compatibility
###############

Tests are passing on Mac OS X and Ubuntu, see recent test runs in `Actions <https://github.com/cfeenstra67/pylumi/actions>`_ for details.

Right now `statey` is only tested with Python 3.8. There are known imcompatabilities with Python 3.6, and they should be addressed. Python 3.7 has not been tested but may very well work as intended.


Contact
#########

If you have issues using this repository please open a issue or reach out to me at cameron.l.feenstra@gmail.com.
