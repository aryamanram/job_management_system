(1) pip install -r requirements.txt

(2) python -m job_submission.submit_job --kernel test_kernel.txt --data test_data.json --backend s3 --bucket v22wusrsdx --endpoint-url https://s3api-us-ks-2.runpod.io --region US-KS-2
(S3 Upload Command)

OR

(2) python -m job_submission.submit_job --kernel test_kernel.txt --data test_data.json
(Local Upload Command)