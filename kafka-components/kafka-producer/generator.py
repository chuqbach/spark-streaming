from faker import Faker
import random
from datetime import datetime
import uuid
import json


class Generator:
    fake = Faker()

    def __init__(self):
        self.eventId = str(uuid.uuid4())
        self.eventTime = datetime.now().strftime("%Y-%m-%d %H:%m:%S")
        self.eventType = "OBOD"
        self.partnerCode = "MOMO"
        self.platform = "OpenAPI"
        self.verificationId = str(uuid.uuid4())
        self.codeId = random.randint(10000000, 99999999)
        self.status = random.choice(["MATCHED", "NOT_MATCH"])
        self.validationCount = random.randint(0, 9)
        self.name = self.fake.name()
        self.phone = self.fake.phone_number()
        self.id = random.randint(1000000000, 9999999999)
        self.dob = self.fake.date_of_birth().strftime("%Y-%m-%d")
        self.resultCode = random.choice(["FOUND", "NOT_FOUND"])
        self.gender = random.choice(["MALE", "FEMALE"])
        self.income = round(random.randint(10000000, 100000000), -6)
        self.externalId = random.randint(100000, 999999)
        self.statusCode = random.choice(["OFFER_AVAILABLE", "NO_OFFER"])
        self.ticketId = str(uuid.uuid4())

    def customer_pre_check(self):
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
            "step": "CUSTOMER_PRE_CHECK",
            "eventId": self.eventId,
            "partnerCode": self.partnerCode,
            "platform": self.platform

        }
        json_data = json.dumps(data)
        return json_data

    def otp_request(self):
        data = {
            "requestLog": {
                "phoneNumber": self.phone
            },
            "responseLog": {
                "verificationID": self.verificationId
            },
            "eventTime": self.eventTime,
            "eventType": self.eventType,
            "step": "OTP_REQUEST",
            "eventId": self.eventId,
            "partnerCode": self.partnerCode,
            "platform": self.platform
        }
        json_data = json.dumps(data)
        return json_data

    def otp_verify(self):
        data = {
            "requestLog": {
                "verificationID": self.verificationId,
                "codeID": self.codeId
            },
            "responseLog": {
                "status": self.status,
                "validationCount": self.validationCount
            },
            "eventTime": self.eventTime,
            "eventType": self.eventType,
            "step": "OTP_VERIFY",
            "eventId": self.eventId,
            "partnerCode": self.partnerCode,
            "platform": self.platform
        }
        json_data = json.dumps(data)
        return json_data

    def obod_result(self):
        data = {
            "requestLog": {

            },
            "responseLog": {
                "phoneNumber": self.phone,
                "identificationNumber": self.id,
                "customerFullName": self.name,
                "birthDate": self.dob,
                "gender": self.gender,
                "income": self.income,
                "externalId": self.externalId,
                "statusCode": self.statusCode,
                "ticketId": self.ticketId,
            },
            "eventTime": self.eventTime,
            "eventType": self.eventType,
            "step": "OFFER_CALCULATION_SUBMISSION",
            "eventId": self.eventId,
            "partnerCode": self.partnerCode,
            "platform": self.platform
        }
        json_data = json.dumps(data)
        return json_data

    def generate_data(self):
        # callable_methods = [func for func in dir(Generator)
        # if callable(getattr(Generator, func)) and not func.startswith("__")]
        callable_methods = [self.customer_pre_check, self.otp_request, self.otp_verify, self.obod_result]
        func = random.choice(callable_methods)
        return func()
