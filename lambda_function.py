import boto3
import os
import mimetypes


# returns a list of all files in the branch
def get_entire_blob_list(codecommit, repository, afterCommitSpecifier):
    response = codecommit.get_differences(
        repositoryName=repository,
        afterCommitSpecifier=afterCommitSpecifier,
    )

    blob_list = [difference['afterBlob'] for difference in response['differences']]
    while 'nextToken' in response:
        response = codecommit.get_differences(
            repositoryName=repository,
            afterCommitSpecifier=afterCommitSpecifier,
            nextToken=response['nextToken']
        )
        blob_list += [difference['afterBlob'] for difference in response['differences']]

    return blob_list


# returns a list of changed files between beforeCommitSpecifier and afterCommitSpecifier
def get_blob_list(codecommit, repository, beforeCommitSpecifier, afterCommitSpecifier):
    response = codecommit.get_differences(
        repositoryName=repository,
        beforeCommitSpecifier=beforeCommitSpecifier,
        afterCommitSpecifier=afterCommitSpecifier,
    )

    blob_list = [difference['afterBlob'] for difference in response['differences']]
    while 'nextToken' in response:
        response = codecommit.get_differences(
            repositoryName=repository,
            beforeCommitSpecifier=beforeCommitSpecifier,
            afterCommitSpecifier=afterCommitSpecifier,
            nextToken=response['nextToken']
        )
        blob_list += [difference['afterBlob'] for difference in response['differences']]

    return blob_list


# lambda-function
# triggered by changes in a codecommit repository
# reads files in the repository and uploads them to s3-bucket
#
# ENVIRONMENT VARIABLES:
#     s3BucketName
#     codecommitRegion
#     repository
#
# TIME OUT: 1 min
#
# EXECUTION ROLE
#     lambda-codecommit-s3-execution-role (permissions: AWSCodeCommitReadOnly, AWSLambdaExecute, AmazonSSMFullAccess)
#
def lambda_handler(event, context):
    repository_name = os.environ['repository']

    # Current HEAD SHA-1 id
    head = event['Records'][0]['codecommit']['references'][0]['commit']
    # source codecommit
    codecommit = boto3.client('codecommit', region_name=os.environ['codecommitRegion'])

    ssmClient = boto3.client('ssm')
    ssmParamName = repository_name + "_beforeCommitSpecifier"
    try:
        # Previous HEAD SHA-1 id (i.e. the commit right before the HEAD when this function is called)
        beforeCommitSpecifier = ssmClient.get_parameter(Name=ssmParamName)['Parameter']['Value']
        blobList = get_blob_list(codecommit, repository_name, beforeCommitSpecifier, head)
    except ssmClient.exceptions.ParameterNotFound:
        # If beforeCommitSpecifier is not set, blobList retrieves a list of the entire files
        blobList = get_entire_blob_list(codecommit, repository_name, head)

    # target bucket
    bucket = boto3.resource('s3').Bucket(os.environ['s3BucketName'])

    # reads each file in the branch and uploads it to the s3 bucket
    for blob in blobList:
        path = blob['path']
        content = (codecommit.get_blob(repositoryName=repository_name, blobId=blob['blobId']))['content']

        # we have to guess the mime content-type of the files and provide it to S3 since S3 cannot do this on its own.
        content_type = mimetypes.guess_type(path)[0]
        if content_type is not None:
            bucket.put_object(Body=(content), Key=path, ContentType=content_type)
        else:
            bucket.put_object(Body=(content), Key=path)

    # Update beforeCommitSpecifier environment variable with the current HEAD
    ssmClient.put_parameter(Name=ssmParamName, Type='String', Value=head, Overwrite=True)
