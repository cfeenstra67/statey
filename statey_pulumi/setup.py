import os

# from distutils.core import setup, Extension
from setuptools import setup, Extension

from Cython.Build import cythonize


current_dir = os.path.dirname(__file__)

go_build_dir = os.path.realpath(os.path.join(current_dir, 'build/go'))

setup(
    name='stalumi',
    version='1.0',
    description='abc',
    # build_golang={'root': 'stalumiconnector'},
    # setup_requires=['setuptools-golang'],
    ext_modules=cythonize(
        [
            # Extension('stalumigo', ['go/connector/main.go']),
            Extension(
                '_stalumi',
                # libraries=['stalumigo'],
                sources=['_stalumi.pyx'],
                include_dirs=[go_build_dir],
                extra_link_args=[f'-Wl,-rpath,{go_build_dir}'],
                # library_dirs=[go_build_dir],
                # runtime_library_dirs=[go_build_dir],
                extra_objects=[os.path.join(go_build_dir, 'libstalumigo.so')],
                extra_compile_args=['-fPIC']
            )
        ]
    )
)
