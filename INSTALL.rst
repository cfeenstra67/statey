Installation
=============

``statey`` and all of its core extension modules can be installed using the following terminal command:

.. code-block:: bash

   $ pip install statey[all]

NOTE: this does **not** install the development-related extras ``dev`` and ``tests``.

To install only the core ``statey`` engine the command is:

.. code-block:: bash

    $ pip install statey

There are a number of possible ``extras`` to install, depending on the use-case:

- ``fmt`` - This enables the ``st.f('{some_ref.abc} and other things')`` format-string function.
- ``pickle`` - This adds extra ``pickle`` extension packages so that functions can be more reliably pickled.
- ``cli`` - This include requirements required to run the statey CLI.
- ``pulumi`` - This includes requirements to utilize pulumi resourc providers. If you do not plan to use pulumi resource providers it would be a good idea not to include this dependency, as installation requires having Go installed on your system and compiling an extension module. See the `pylumi documentation <https://pylumi.readthedocs.io/en/latest/?badge=latest#installation>`_ for details.
- ``all`` - This includes requirements from all of the above.
- ``core`` - This includes the requirements from the ``fmt`` and ``cli`` extras.
- ``tests`` - This includes requirements to run the tests. NOTE: this is _not_ included by default in ``state[all]``.
- ``dev`` - This includes miscellaneous requirements required for development such as `black`. NOTE: this is _not_ included by default in ``statey[all]``.
