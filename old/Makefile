install:
	python -m pip install -r requirements.txt

test:
	python production_smoke_test.py
	python smoke_test.py

run:
	uvicorn backend:app --reload

docker:
	docker compose up --build
