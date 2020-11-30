.. _Basic Tutorial:

================
Basic Tutorial
================

This tutorial walks through creation of resources with a simple ``statey`` project and discusses some of the core concepts to understand along the way.

.. note::
    
    This is an extension of the example from :ref:`Getting Started`.

.. note:: 
    
    To run this example you must have the pulumi aws provider installed. This can be done by running the following if it is not already installed:

    .. code-block:: bash

        $ statey install pulumi/aws==2.13.1

A Statey Module file
##########################

A typical statey module might look something like the following (in a file called ``statey_module.py``):

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

Let's take a brief moment to break down what's happening here; as ``statey`` puts an emphasis on expressiveness in its design, there is actually a good amount happening in this little bit of code:
- Line 1: ``import statey as st`` - This is just a regular import of the top-level ``statey`` module. ``st`` is the convention abbreviation to use for the import.
- Line 2: ``from statey.ext.pulumi.providers import aws`` - While this may appear to also be a typical import of the ``aws`` module from the ``statey.ext.pulumi.providers`` package, actually this is a custom import hook to import the pulumi ``aws`` provider API, which can be used to easily instantiate any sort of ``aws`` resource like you see here with some S3 examples. Note that the following code blocks have the same effect as this line:

.. code-block:: python

    from statey.ext.pulumi import providers
    aws = providers.aws
    aws = providers.get_provider('aws')

.. code-block:: python

    import statey as st
    from statey.ext.pulumi.provider_api import ProviderAPI
    aws_provider = st.registry.get_provider("pulumi/aws")
    aws = ProviderAPI("aws", aws_provider)


- Line 4: ``@st.declarative`` - This decorator enables a particularly simple API so that the names we give each resource in Python map to their names in ``statey``. This is not necessary, and only exists in the name of expressiveness. The following code has an identical effect:

.. code-block:: python

    def module(session):
        bucket = session["bucket"] << aws.s3.Bucket(
            bucket='my-bucket-name'
        )
        object_1 = session["object_1"] << aws.s3.BucketObject(
            bucket=bucket.bucket,
            key='file-1.json',
            source='./static/file1.json',
            contentType='application/json'
        )
        object_2 = session["object_2"] << aws.s3.BucketObject(
            bucket=bucket.bucket,
            key='file-2.txt',
            content=st.f('This is in a bucket named {bucket.bucket}')
        )

Ultimately the goal of any statey module is to modify a ``Session`` object and add resources and/or data to it as desired. The ``st.declarative`` decorator just automatically binds the locals of the decorated function to names in the session.

- Line 5-end: ``def module(session):`` content - this is the actual functional code of the module. This sets us a ``statey`` session with resources (and optionally data) keys, which is then used alongside the existing ``ResourceGraph`` from prior operations (or an empty one if no operation has yet been applied) to create the plan that is displayed when running ``statey plan`` or ``statey up``. Note that if you use the ``st.declarative`` decorator, any name beginning with ``_`` within the session will not be added to the session, so you can use names like this for temporary values or for holding references after adding names to the session directly (as seen in the previous bullet using the ``<<`` operator).

In order to inspect the available resources and their different types, there are two main methods:

- Depending on the specifics, the relevant Python objects may be directly inspectable. For example, the native ``dir()`` function can be called on ``statey.ext.pulumi.providers.aws`` objects to see a listing of the available submodules such as ``s3``, ``ec2``, and many others. The individual module API objects such as ``aws.s3`` and ``aws.ec2`` are also inspectable for the available resource types (``aws.s3.Bucket`` and ``aws.s3.BucketObject`` in this example).

- There is a ``statey docs`` command that allows for simple inspection of providers and their resources in general. ``statey docs --help`` can provide specific options available, but the following could be used to list all available resource names for the ``aws`` provider:

.. code-block:: bash

    $ statey docs pulumi/aws

This can be used to inspect all available resource names from the provider, one per line. Perhaps you find something interesting, such as ``aws:athena/database:Database``, and want to inspect it further. To do this at the command line, just run the following:

.. code-block:: bash

    $ statey docs pulumi/aws -r aws:athena/database:Database

Now, the following code block shows how to access these objects directly in Python:

.. code-block:: python

    import statey as st

    aws_provider = st.registry.get_provider("pulumi/aws")
    Database = aws_provider.get_resource("aws:athena/database:Database")

    @st.declarative
    def module(session):
        db = Database(
            bucket='my-bucket-name',
            name='my-db-name',
            forceDestroy=True
        )
        ...

The interface is similar to the shortcut of importing from ``statey.ext.pulumi.providers``. Once your module is ready, you are ready to actually create the physical resources you've defined with ``statey up``.

Creating your resources with ``statey up``
############################################

To use the ``statey`` command line tool, you should be in the same directory as your ``state_module.py`` file.

In order to use the ``aws`` provider, you must set a minimum configuration of the current region. The more general way of doing this will be discussed below, but for ease of getting started statey supports setting this property through the environment using the ``AWS_DEFAULT_REGION`` or ``AWS_REGION`` environment variables (``AWS_REGION`` takes precedence if they are both set). So simply run the following before getting started:

.. code-block:: bash

    $ export AWS_DEFAULT_REGION=<my_default_region>

Next, simply run the following in the same directory as your ``statey_module.py`` file:

.. code-block:: bash

    $ statey up

Your output should resemble the following:

.. code-block::

    Planning completed successfully.

    Task DAG:

    *-.   bucket1:task:create
    |\ \  
    * | | object3:task:create
     / /  
    * | object1:task:create
    |/  
    * object2:task:create

    Resource summary: 4 to create, 0 to update, 0 to delete.

The program will ask for confirmation, and if it is not given it will abort. If it is, it will execute the tasks as shown in the graph. If the configuration is valid, all should end in success. If any of your resources fails to create, don't worry--attempt to fix the error in the configuration and run ``statey up`` again, and your infrastructure will be updated incrementally to the desired structure.

If you want more detailed information about the resources that will be updated, use the ``--diff`` command line argument e.g. ``statey up --diff``.

If you run ``statey up`` again, you should see:

.. code-block:: 

    This plan is empty :)

If you want to tear your infrastructure back down, simply run ``statey down`` to do so. Once again you'll see a Task DAG, be asked for confirmation, and if it is given the tasks will execute in the correct order and all of your infrastructure will be torn down cleanly.

Alternatively, if you want to make changes to your configuration, you can run ``statey up`` and ``statey`` will execute the operations required to update your resources incrementally to the desired configuration (including adding/deleting resources).

Configuration via ``statey_conf.py``
#####################################

The primary mechanism of customizing and configuring behavior in ``statey`` is via hooks. A wide array of hooks are available to introduce and configure most objects introduced into ``statey``, and hooks are underlying most of the `Registry` methods. The :ref:`Hooks` reference should be read for more detail on available hooks and typical usage, but for the purposes of this tutorial we will not go deep into those details.

The important point is that for everything from adding behavior to ``statey`` to configuring default provider configuration, hooks must be registered before the code in ``state_module.py``'s ``module()`` method runs, and for simplicity even before ``statey_module.py`` runs at all.

For this purpose, another python module may exist in the same directory called ``statey_conf.py`` whose content will always be run before ``statey_module.py``. This is optional, as hooks may also be registered at the beginning of ``statey_module.py`` if desired, but keeping this separation is desirable in many cases.

A simple example of a ``statey_conf.py`` could be the following:

.. code-block:: python

    import statey as st

    st.helpers.set_provider_defaults("pulumi/aws", {"region": "<my_region_name>"})

Under the hood, the ``st.helpers.set_provider_defaults()`` function registers a plugin that implements the ``get_provider_config()`` hook to achieve behavior analogous to "setting defaults". If you need to set up additional providers or register custom plugins and/or resources, this module is that place to do that as well.

Code
#####

The code for this tutorial can be found on `Github <https://github.com/cfeenstra67/statey/examples/projects/aws-simple>`_.
