# -*- encoding: utf-8 -*-

import json
import base64

class DummyEncoder(object):
    """
    Dummy Encoder, return the values unchanged
    """
    def encode(self, message, headers):
        """Encodes a message"""
        #headers[self.encoderheader] = self.name
        return message, headers

    def decode(self, message, headers):
        """Decodes a message"""
        return message, headers

class JSONEncoder(object):
    """
    Every message sent on a connector pass through an encoder before being sent or dispatched
    """

    def encode(self, message, headers):
        """Encodes a message using base64 json"""
        if headers:
            encodedheaders = {}
            for key, val in headers.items():
                if isinstance(key, unicode):
                    key = key.encode('utf-8')
                if isinstance(val, unicode):
                    val = val.encode('utf-8')
                encodedheaders[key] = val
            headers = encodedheaders
        return base64.b64encode(json.dumps(message)), headers

    def decode(self, message, headers):
        """Decodes a message encoded by the same encoder"""
        try:
            return json.loads(base64.b64decode(message)), headers
        except TypeError, exc:
            logger.warning("Message decoding error: %s" % str(exc))
            logger.exception(exc)
