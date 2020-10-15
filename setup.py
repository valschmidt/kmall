from setuptools import setup, find_packages


setup(
    name = "kmall",
    version = "1.0.0",
    license = "CC0 1.0 Universal",
    packages = find_packages(),
    python_requires=">=3.0",
    install_requires=[
        "pandas",
        "numpy",
        "pyproj",
        "scipy",
    ],
    description="Library and apps for reading Kongsberg sonar KMALL data files.",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Natural Language :: English",
        "License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Scientific/Engineering :: GIS"
    ],
    entry_points={
        'console_scripts': ['kmall.py=KMALL.kmall:main']
    },
    keywords = "multibeam hydrography ocean mapping acoustic data",
    author = "Val Schmidt; Lynette Davis; Eric Younkin",
    author_email="vschmidt@ccom.unh.edu, ldavis@ccom.unh.edu, eric.g.younkin@noaa.gov"

)
