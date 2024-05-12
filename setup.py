from setuptools import find_packages, setup

setup(
    name="syndiffix_tools",
    version="0.0.2",
    packages=find_packages(exclude=["tests"]),
    description="A variety of tools for managing SynDiffix synthetic data.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Open Diffix",
    author_email="hello@open-diffix.org",
    url="https://github.com/diffix/syndiffix_tools",
    install_requires=["syndiffix", "pyarrow"],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
)
