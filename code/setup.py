from setuptools import setup

setup(
    name="food-orders-beam",
    version="0.1",
    install_requires=[
        "apache-beam[gcp]==2.54.0",
        "google-cloud-bigquery"
    ],
)