(1) pip install -r requirements.txt

(Submit Job)
(2) python -m job_submission.submit_job --kernel test_kernel.txt --data test_data.json

(Worker Pull)
(3) python -m worker.run_worker --once 

(3) python -m worker.run_worker


python -m job_submission.get_job --uuid