from setuptools import setup

setup(
    name="TradierLib",
    version="0.1.4",
    author="Jason",
    author_email="",
    description="Tradier Data API",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/toaster-robotics/TradierLib",
    py_modules=["TradierLib"],
    install_requires=[
        "requests>=2.0.0",
        "pandas>=2.0.0"
    ],
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
