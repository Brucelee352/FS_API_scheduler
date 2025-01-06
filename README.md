# Introduction

This project serves as a guide on how to produce business-centric product data as JSON, for containerized data ingestion; and further orchestration using Docker, MinIO and Python. I'm looking to make this as simple as possible, while being applicable to real-world business scenarios and use-cases.

# Data Sourcing

The central piece of this project is the Python script that produces the data itself. I'm specifically generating columns that reflect how real-world users interact with transactional databases; relying on the following assumptions:

- Their data has comprehensive metadata, including the date and time of the transaction, the user's ID, the product's ID, and the price of the product.
