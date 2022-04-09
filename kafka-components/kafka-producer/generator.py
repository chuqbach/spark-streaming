from faker import Faker
import random
from datetime import datetime
import uuid
import json


class CustomerPreCheck:
    fake = Faker()

    def __init__(self):
        self.eventId = str(uuid.uuid4())
        self.eventTime = datetime.now().strftime("%Y-%m-%d %H:%m:%S")
        self.eventType = "OBOD"
        self.step = "CUSTOMER_PRE_CHECK"
        self.partnerCode = "MOMO"
        self.platform = "OpenAPI"
        self.name = self.fake.name()
        self.phone = self.fake.phone_number()
        self.id = random.randint(1000000000, 9999999999)
        self.resultCode = random.choice(["FOUND", "NOT_FOUND"])

    def generate_data(self):
        data = {
            "requestLog": {
                "phoneNumber": self.phone,
                "identificationCardNumber": self.id
            },
            "responseLog": {
                "resultCode": self.resultCode
            },
            "eventTime": self.eventTime,
            "eventType": self.eventType,
            "step": self.step,
            "eventId": self.eventId,
            "partnerCode": self.partnerCode,
            "platform": self.platform

        }
        json_data = json.dumps(data)
        return json_data


class OTPRequest:
    fake = Faker()

    def __init__(self):
        self.eventId = str(uuid.uuid4())
        self.eventTime = datetime.now().strftime("%Y-%m-%d %H:%m:%S")
        self.eventType = "OBOD"
        self.step = "CUSTOMER_PRE_CHECK"
        self.partnerCode = "MOMO"
        self.platform = "OpenAPI"
        self.name = self.fake.name()
        self.phone = self.fake.phone_number()
        self.id = random.randint(1000000000, 9999999999)
        self.resultCode = random.choice(["FOUND", "NOT_FOUND"])

    def generate_data(self):
        data = {
            "requestLog": {
                "phoneNumber": self.phone,
                "identificationCardNumber": self.id
            },
            "responseLog": {
                "resultCode": self.resultCode
            },
            "eventTime": self.eventTime,
            "eventType": self.eventType,
            "step": self.step,
            "eventId": self.eventId,
            "partnerCode": self.partnerCode,
            "platform": self.platform

        }
        json_data = json.dumps(data)
        return json_data
