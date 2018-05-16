# RDSS EPrints Adaptor

## Introduction

The RDSS Eprints Adaptor is a per-institutional adaptor for use by institutions that utilise an instance of the [EPrints open access repository](http://www.eprints.org/).

The adaptor will execute periodically, querying the OAI-PMH endpoint exposed by the EPrints instance to retrieve records that have been added to the repository since the creation timestamp of the most recently retrieved record. Once the record is retrieved, its corresponding digital objects are retrieved and stored in an S3 bucket, and its metadata is converted into a format compliant with the Jisc RDSS canonical data model. It is then published into the messaging system, for consumption by downstream systems.

The EPrints Adaptor is capable of interacting with any OAI-PMH compliant endpoint.

## Language / Framework

* Python 3.6+
* Docker

## Service Architecture

The adaptor runs as a Docker container which can be configured to point to the EPrints OAI-PMH endpoint. It also requires DynamoDB tables and S3 buckets to operate - all of this infrastructure is created through the [accompanying Terraform](https://github.com/JiscRDSS/rdss-institutional-ecs-clusters/tree/develop/infra-eprints-adaptor/tf).

The following environmental variables are required for the adaptor to run. These are typically provided as parameters to the Docker container:

* `EPRINTS_JISC_ID`
  * The Jisc ID of the institution that is operating the EPrints instance.

* `EPRINTS_ORGANISATION_NAME`
  * The name of the institution that is operating the EPrints instance.

* `EPRINTS_EPRINTS_URL`
  * The URL endpoint of the OAI-PMH endpoint of the EPrints instance.

* `EPRINTS_DYNAMODB_WATERMARK_TABLE_NAME`
  * The name of the DyanmoDB table where the high watermark of the adaptor is persisted.

* `EPRINTS_DYNAMODB_PROCESSED_TABLE_NAME`
  * The name of the DyanmoDB table where the status of processed EPrints records are recorded.

* `EPRINTS_S3_BUCKET_NAME`
  * The name of the S3 bucket used to persist the digital objects retrieved from the EPrints instance.

* `EPRINTS_OUTPUT_KINESIS_STREAM_NAME`
  * The name of the Kinesis stream where the converted EPrints records are pushed to for consumption by downstream systems.

* `EPRINTS_OUTPUT_KINESIS_INVALID_STREAM_NAME`
  * The name of the Kinesis stream where invalid generated messages are pushed to.

* `EPRINTS_API_SPECIFICATION_VERSION`
  * The version of the Jisc RDSS API specification that generated messages are compliant with.

## Developer Setup

To run the adaptor locally, configure all the required environmental variables described above. To create the local virtual environment, install dependencies and manually run the adaptor:

```
make env
source ./env/bin/activate
make deps
python run.py
```

### Testing

To run the test suite for the RDSS Eprints Adaptor, run the following command:

```
pytest
```
