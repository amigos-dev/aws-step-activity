# aws-step-activity
aws-step-activity: Easy AWS step function state machines and activities
=================================================

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Latest release](https://img.shields.io/github/v/release/amigos-dev/aws-step-activity.svg?style=flat-square&color=b44e88)](https://github.com/amigos-dev/aws-step-activity/releases)

A commandline tool and API for creating and managing simple AWS stepfunction state machines
and implementing stepfunction activity task workers.

Table of contents
-----------------

* [Introduction](#introduction)
* [Installation](#installation)
* [Usage](#usage)
  * [Command line](#command-line)
  * [API](api)
* [Known issues and limitations](#known-issues-and-limitations)
* [Getting help](#getting-help)
* [Contributing](#contributing)
* [License](#license)
* [Authors and history](#authors-and-history)


Introduction
------------

Python package `aws-step-activity` provides a command-line tool as well as a runtime API for managing and accessing
AWS stepfunction state machines, activities, and executions, and for implementing activity task workers.

Some key features of aws-step-activity:

* Provides a base class for activity workers that can download input files from S3 and upload result files to S3.
* Provides a built-in functional activity worker that can run arbitrary shell scripts and capture stdout/stderr.
* Provides helpers for signed S3 upload/download so that a remote API client can directly transfer artifacts to/from S3
* Convenient CLI tools for creation of activity-selecting state machines, and for adding/removing activities.
* A rich command-line tool:
  * JSON-format for all command results
  * Optional colored output
  * Optional raw (unquoted) results for string and binary data


Installation
------------

### Prerequisites

**Python**: Python 3.7+ is required. See your OS documentation for instructions.

**AWS Client**: boto3 is used for AWS access. A proper AWS profile with credentials must be configured. If the profile is not _default_, it can be selected with environment variable AWS_PROFILE or provided to the API or on the command line. The AWS CLI tool is not required, but it is recommended.

### From GitHub

[Poetry](https://python-poetry.org/docs/master/#installing-with-the-official-installer) is required; it can be installed with:

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

Clone the repository and install aws-step-activity into a private virtualenv with:

```bash
cd <parent-folder>
git clone https://github.com/migos-dev/aws-step-activity.git
cd aws-step-activity
poetry install
```

You can then launch a bash shell with the virtualenv activated using:

```bash
poetry shell
```

Example
========

In this example, you will run a simple script activity worker in terminal 1, then submit an execution to it
aand view the results in terminal 2. Note that terminal 1 and terminal 2 may
be run on physically distinct machines.

In these examples, an attempt is made to generate unique AWS resource names (S3 bucket, AWS step function state machine name),
so as not to collide with others that may run the same example in the same AWS account. You may freely
modify the environment variables to reuse existing S3 bucket, etc.

These examples make use of the _jq_ commandline utility as well as the AWS CLI.

### In terminal 1:
```bash
cd ~
mkdir -p aws-step-activity-test/worker
cd aws-step-activity-test/worker
git clone https://github.com/migos-dev/aws-step-activity.git
cd aws-step-activity
poetry install
poetry shell
# following commands run in poetry subshell
export AWS_PROFILE=default  # replace with the AWS profile you want to use
export ACTIVITY="$USER-test-activity"
export STATE_MACHINE="$ACTIVITY-state-machine"
aws-step-activity -m "$STATE_MACHINE" create-activity-chooser --default "$ACTIVITY" "$ACTIVITY"
aws-step-activity -m "$STATE_MACHINE" describe-activity-chooser
aws-step-activity --tb --loglevel=debug -a "$ACTIVITY" run
# leave running, and open terminal 2
```

### In terminal 2:
```bash
cd ~
mkdir -p aws-step-activity-test/client
cd aws-step-activity-test/client
git clone https://github.com/migos-dev/aws-step-activity.git
cd aws-step-activity
poetry install
poetry shell
# following commands run in poetry subshell
export AWS_PROFILE=default  # replace with the AWS profile you want to use
export EXECUTION_S3_BUCKET="test-bucket-$(aws sts get-caller-identity | jq -r .Account)-$USER"

mkdir -p test_data
cd test_data
export ACTIVITY="$USER-test-activity"
export STATE_MACHINE="$ACTIVITY-state-machine"
export EXECUTION_S3_KEY_PREFIX="$USER-test-activity"
aws s3api create-bucket --bucket "$EXECUTION_S3_BUCKET" --create-bucket-configuration LocationConstraint="$(aws configure get region)"
export EXECUTION_S3_URL_PREFIX="s3://$EXECUTION_S3_BUCKET/$EXECUTION_S3_KEY_PREFIX"

# The following should be repeated for each execution submitted
export EXECUTION_NAME="$(aws-step-activity -r gen-execution-name)"

# create a script to run in the worker
cat  >script.sh <<EOF
#!/bin/bash
set -e
pwd
find .
echo "sleeping for 20 seconds"
sleep 20
cat ./input/input_data.txt
echo "This is a test output file" > ./output/test_output_file.txt
EOF

rm -fr ./inputs
mkdir inputs

# provide an input file to the script that is passed through S3
cat >inputs/input_data.txt <<EOF
This is a test input file!
EOF

# start an execution running
aws-step-activity --tb -m "$STATE_MACHINE" -s "$EXECUTION_S3_URL_PREFIX" start-execution --name="$EXECUTION_NAME" -i inputs -v script@=script.sh

# Wait for execution to finish
aws-step-activity --tb -m "$STATE_MACHINE" wait-for-execution "$EXECUTION_NAME"

# Print stdout
aws-step-activity -m "$STATE_MACHINE" cat-execution-output "$EXECUTION_NAME" stdout.txt

# Print stderr
aws-step-activity -m "$STATE_MACHINE" cat-execution-output "$EXECUTION_NAME" stderr.txt >&2

# Print the generated output file
aws-step-activity -m "$STATE_MACHINE" cat-execution-output "$EXECUTION_NAME" test_output_file.txt
```


Usage
=====

Command Line
------------

There is a single command tool `aws-step-activity` that is installed with the package.

### Running a generic activity task worker

To start a generic activity task worker that can run arbitrary shell scripts:

```bash

cd <worker-data-parent-dir>
aws-step-activity [-p <AWS-profile>] [-r <AWS-region>]  -a <AWS-stepfunction-activity-name> -w <unique-worker-name> run
```

The task worker will keep running until it is stopped; e.g., with Ctrl-C.


API
---

TBD

Known issues and limitations
----------------------------

TBD

Getting help
------------

Please report any problems/issues [here](https://github.com/amigos-dev/aws-step-activity/issues).

Contributing
------------

Pull requests welcome.

License
-------

aws-step-activity is distributed under the terms of the [MIT License](https://opensource.org/licenses/MIT).  The license applies to this file and other files in the [GitHub repository](http://github.com/amigos-dev/aws-step-activity) hosting this file.

Authors and history
---------------------------

The initial author of aws-step-activity is [Sam McKelvie](https://github.com/sammck).
It is maintained by [amigos.dev](https://amigos.dev).
