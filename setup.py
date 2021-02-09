import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="project", # Replace with your own username
    version="0.0.1",
    author="Kacper Domzal",
    description="Final assigment in DiP",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    packages=setuptools.find_packages(), #automatically discover all packages and subpackages. in this instance only "project"
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'numpy',
        'pandas',
        'matplotlib',
        'requests',
        'geopy',
        'datetime',
        ]
)
