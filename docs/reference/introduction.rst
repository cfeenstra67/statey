Introduction
=============

Statey has a few core data structure that are important to understand in using and extending the library. In order to do so, it's useful to see how it can be used outside of the command line or any sort of resources to store a set of codependent objects.

The first two data structure to be aware of are :ref:`Objects` and :ref:`Sessions`. A statey ``Object`` is the primary manner that statey represents declaratively related data. It acts a little bit like a promise, a Spark dataset, or other functionally-related objects in other languages and frameworks. Every statey object has two things: a `type` and an `implementation`. The primary way to create statey objects is using ``st.Object()`` or by interacting with other statey objects. For example:

.. code-block:: python

    >>> import statey as st
    >>> obj1 = st.Object(1)
    >>> print("OBJ", obj1)        # Data(1)
    >>> print("TYPE", obj1._type) # integer
    >>> print("IMPL", obj1._impl) # Data(value=1)

    >>> obj2 = obj1 + 1
    >>> print("OBJ", obj2)        # NativeFunctionCall(__add__(other=Data(1), inst=Data(1)))
    >>> print("TYPE", obj2._type) # integer
    >>> print("IMPL", obj2._impl) # FunctionCall(func=NativeFunction(type=λ[[inst:integer, other:integer] => integer], name='__add__'), arguments={'other': Data(1), 'inst': Data(1)})

You can easily write your own typed functions to use with statey objects by using the ``st.function`` decorator, like so:

.. code-block:: python

    import statey as st

    @st.function
    def my_add(first: int, second: int, third: int) -> int:
        return first + second + third

    ...

    # The function can be inspected
    >>> my_add.type                   # λ[[first:integer, second:integer, third:integer] => integer]
    >>> obj3 = my_add(obj1, obj2, 17) 
    >>> print("OBJ", obj3)            # NativeFunctionCall(my_add(first=Data(1), third=Data(17), second=NativeFunctionCall(__add__(other=Data(1), inst=Data(1)))))
    >>> obj3 = my_add(obj1, obj2, object()) # NOTE: this throws a validation error!

A ``Session`` refers to a namespace that may or may not data associated with its keys. It is also the way to resolve concrete object values. A standard session can be created with the ``st.create_session`` method, and the primary way to insert data into a session is using the ``<<`` operator, which will return a session reference rather than just the object being inserted. An example of basic session usage can be seen below:

.. code-block:: python

    import statey as st
    
    session = st.create_session()

    ref_a = session["a"] << st.Object(2) + 1
    ref_b = session["b"] << 12
    ref_c = session["c"] << ref_a + ref_b

    ...

    >>> print("REFS", ref_a, ref_b, ref_c) # Reference(a) Reference(b) Reference(c)
    >>> session.resolve(st.Object(1))      # 1
    >>> session.resolve(ref_a)             # 3
    >>> session.resolve(ref_b)             # 12
    >>> session.resolve(ref_c)             # 15
    # Updating data in the sesssion
    >>> session.set_data("a", 101)
    # Resolving dependent objects makes use
    # of the new value!
    >>> session.resolve(ref_c)             # 113
