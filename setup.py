from setuptools import setup, find_packages

with open("Readme.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name = "pykmall",
    version = "1.1.1",
    license_expression = "BSD-2-Clause",
    packages = find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pandas",
        "numpy",
        "pyproj",
        "scipy",
    ],
    description="Library and apps for reading Kongsberg sonar KMALL data files.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/valschmidt/kmall",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering :: GIS"
    ],
    entry_points={
        'console_scripts': ['kmall=KMALL.kmall:main',
                            'kmall.py=KMALL.kmall:main']
    },
    keywords = "multibeam hydrography ocean mapping acoustic data",
    author = "Val Schmidt; Lynette Davis; Eric Younkin",
    author_email="vschmidt@ccom.unh.edu, ldavis@ccom.unh.edu, eric.g.younkin@noaa.gov"

)
