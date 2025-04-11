import requests

SUBMIT_LOAN = "http://127.0.0.1:5000/submit-loan"
GET_LOAN = "http://127.0.0.1:5000/loan-status/1"
POST_LOAN_EVENTS = "http://127.0.0.1:5000/publish/loan-events"
POST_LOAN_APPROVALS = "http://127.0.0.1:5000/publish/loan-approvals"
POST_LOAN_REJECTIONS = "http://127.0.0.1:5000/publish/loan-rejections"

if __name__ == "__main__":

    payload = {"loan_id": "LOAN-002",
               "applicant_name": "Nick Huang",
               "loan_amount": "1000"}

    payload_process = {"loan_id": "LOAN-003",
                       "status": "approved"}

    # Submit loan via api
    # response = requests.put(SUBMIT_LOAN, json=payload)
    # data = response.json()
    # print(data["message"])

    # Get the Loan via API
    response = requests.get(GET_LOAN)
    data = response.json()
    print(data)

    # Submit loan approval
    # response = requests.post(POST_LOAN_EVENTS, json=payload_process)
    # data = response.json()
    # print(data)