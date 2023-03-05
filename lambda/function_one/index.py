import http

def handler(event, context):
    # Let's assume you call to a remote endpoint and 
    # received a client exception
    raise http.client.RemoteDisconnected('timeout exceeded')
