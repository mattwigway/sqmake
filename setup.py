import setuptools

setuptools.setup(
    name='sqmake',
    version='0.1.0',
    author='Matthew Wigginton Conway',
    author_email='matt@indicatrix.org',
    description='Makefile-like build management for database-driven workloads',
    url='https://github.com/mattwigway/sqmake',
    packages=setuptools.find_packages(),
        classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    scripts=['sqm']
)
