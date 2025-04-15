from setuptools import setup, find_packages

setup(
    name="economic_data_pipeline",
    version="0.1.0",
    description="Economic Data Pipeline for Azure Automation",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.3.0",
        "numpy>=1.20.0",
        "requests>=2.25.0",
        "python-dotenv>=0.15.0",
        "azure-identity>=1.7.0",
        "azure-keyvault-secrets>=4.3.0",
        "azure-storage-blob>=12.9.0",
        "azure-data-tables>=12.0.0",
        "python-dateutil>=2.8.1"
    ],
)