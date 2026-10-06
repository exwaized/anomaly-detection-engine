from setuptools import setup, find_packages

setup(
    name="anomaly-detection-engine",
    version="1.0.0",
    description="Distributed multi-threaded anomaly detection for business metric streams",
    author="Aditya",
    packages=find_packages(exclude=["tests*"]),
    python_requires=">=3.11",
    install_requires=[
        "pyyaml>=6.0",
    ],
    extras_require={
        "dev": ["pytest>=8.0", "pytest-timeout>=2.3"],
    },
    entry_points={
        "console_scripts": [
            "anomaly-engine=main:main",
        ],
    },
)
