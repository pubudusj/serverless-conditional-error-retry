def handler(event, context):
    # This will raise a TypeError if the key abc
    # is not included in the event payload.
    print(event['abc'])
