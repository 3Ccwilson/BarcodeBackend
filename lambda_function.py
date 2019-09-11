import json
import boto3
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

SECRET_ARN   = 'arn:aws:secretsmanager:us-east-1:796936543418:secret:AuroraDBTemp3-Secret-Ph7LB6'
RESOURCE_ARN = 'arn:aws:rds:us-east-1:796936543418:cluster:dbauroratemp3'

def returnNumberOfRecords(event):
    numberOfRecords = event["numberOfRecordsUpdated"]

    return(numberOfRecords)

def returnRC(event):
    returnCode = event["ResponseMetadata"]
    returnCode = returnCode["HTTPStatusCode"]

    return(returnCode)

def returnBCPin(event):

    returnRecords = event["records"]
    returnsRecords = returnRecords[0]
    returnRecords = returnRecords[0]
    #returnRecords = event["records"][0][0]
    BC = returnsRecords[0]
    BC = BC["stringValue"]
    PIN = returnRecords[1]
    PIN = PIN["stringValue"]

    return(BC, PIN)

def updateBarCode(client, mdn, BC):

    updateString = "UPDATE dbauroratemp3.BARCODES set status = 1, mdn = " + mdn + " where barcode = \"" + BC + "\" AND status = 0"

    response = client.execute_statement(
        secretArn   = SECRET_ARN,
        resourceArn = RESOURCE_ARN,
        sql = updateString
    )

    logger.info(response)

    returnCode = returnRC(response)
    logger.info(returnCode)

    numberOfModifiedRecords = returnNumberOfRecords(response)
    logger.info(numberOfModifiedRecords)

    return(returnCode, numberOfModifiedRecords)

def resetBarCode(client, mdn, BC):

    updateString = "UPDATE dbauroratemp3.BARCODES set status = 0, mdn = NULL where barcode = \"" + BC + "\" AND status = 1"

    response = client.execute_statement(
        secretArn   = SECRET_ARN,
        resourceArn = RESOURCE_ARN,
        sql = updateString
    )

    logger.info(response)

    returnCode = returnRC(response)
    logger.info(returnCode)

    numberOfModifiedRecords = returnNumberOfRecords(response)
    logger.info(numberOfModifiedRecords)

    return(returnCode, numberOfModifiedRecords)

def selectBarCode(client, sku):
    selectString = "SELECT Barcode, PIN FROM dbauroratemp3.BARCODES where sku = \"" + sku + "\" AND status = 0 ORDER BY RAND() LIMIT 1"

    response = client.execute_statement(
        secretArn   = SECRET_ARN,
        resourceArn = RESOURCE_ARN,
        sql = selectString
    )

    logger.info(response)

    returnCode = returnRC(response)

    # We need to check to ensure there is a valid response.
    # The response may be empty if there are no unused bar codes or if the SKU is not available in the DB
    BC, PIN = returnBCPin(response)

    return(returnCode,BC, PIN)


def lambda_handler(event, context):
    # TODO implement

    logger.info(event)
    if event['body']:
        event = json.loads(event['body'])

    sku = event["sku"]
    mdn = event["mdn"]

    client = boto3.client('rds-data')

    returnCode, BC, PIN = selectBarCode(client, sku)

    returnCode, numberOfModifiedRecords = updateBarCode(client, mdn, BC)

    if returnCode == 200 and numberOfModifiedRecords == 1:
        statusCode  = 200
        statusDescription = "OK"
        bodyDetails = { "statusDescription": statusDescription, "Barcode": BC, "PIN": PIN}
    else:
        statusCode = 500
        statusDescription = "Internal Error"
        bodyDetails = { "statusDescription": statusDescription}

    # only needed for testing to reset the data in the table
    returnCode, numberOfModifiedRecords = resetBarCode(client, mdn, BC)
    return {
        'statusCode': statusCode,
        'headers': { 'Content-Type': 'application/json' },
        'body': json.dumps({ 'statusDescription': statusDescription, 'Barcode': BC, 'PIN': PIN })
    }
